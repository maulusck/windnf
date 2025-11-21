# metadata_manager.py
from __future__ import annotations

import bz2
import gzip
import logging
import lzma
import os
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import urljoin

from .config import Config
from .db_manager import DbManager
from .downloader import Downloader

logger = logging.getLogger(__name__)


# ------------------------------------------------------------
# Decompression
# ------------------------------------------------------------


def _decompress_bytes(data: bytes) -> bytes:
    """Try gzip, bz2, xz. Return raw data if none apply."""
    # gzip
    try:
        if data.startswith(b"\x1f\x8b"):
            return gzip.decompress(data)
    except Exception:
        pass
    # bz2
    try:
        if data.startswith(b"BZh"):
            return bz2.decompress(data)
    except Exception:
        pass
    # xz
    try:
        if data.startswith(b"\xfd7zXZ\x00"):
            return lzma.decompress(data)
    except Exception:
        pass

    return data


# ------------------------------------------------------------
# Metadata Manager
# ------------------------------------------------------------


class MetadataManager:
    """
    RPM metadata syncing, but strictly SQLite-first.

    Logic:
        - Always pick `<data type="primary_db">`
        - Download compressed SQLite (.bz2/.gz/.xz)
        - Decompress → validate SQLite header → temp file
        - Import via DbManager.import_repodb
    """

    SQLITE_HEADER = b"SQLite format 3\x00"

    def __init__(self, config: Config, db_manager: DbManager, max_workers: int = 4):
        self.config = config
        self.db = db_manager
        self.downloader = Downloader(self.config)
        self.max_workers = max_workers

    # --------------------------------------------------------
    # Main entry: sync one repo
    # --------------------------------------------------------

    def sync_repo(self, repo_row: Dict[str, Any]) -> None:
        repo_id = repo_row["id"]
        base_url = repo_row["base_url"]
        repomd_href = repo_row["repomd_url"]

        repomd_url = (
            repomd_href
            if repomd_href.startswith("http")
            else urljoin(base_url.rstrip("/") + "/", repomd_href.lstrip("/"))
        )

        logger.info("Sync repo '%s' from %s", repo_row["name"], repomd_url)

        # ----------------------------------------------------
        # Fetch repomd.xml
        # ----------------------------------------------------
        try:
            repomd_bytes = self.downloader.download_to_memory(repomd_url)
        except Exception as e:
            logger.error("Failed to download repomd.xml for %s: %s", repo_row["name"], e)
            return

        # ----------------------------------------------------
        # Locate *actual* primary_db sqlite
        # ----------------------------------------------------
        sqlite_url = self._find_primary_sqlite_url(repomd_bytes, base_url)
        if not sqlite_url:
            logger.error("No <data type=\"primary_db\"> found in repomd.xml — cannot sync repo '%s'", repo_row["name"])
            return

        # ----------------------------------------------------
        # Download + decompress + validate sqlite
        # ----------------------------------------------------
        sqlite_temp = self._download_and_extract_sqlite(sqlite_url)
        if not sqlite_temp:
            logger.error("Failed to prepare sqlite metadata for repo '%s'", repo_row["name"])
            return

        logger.info("Using sqlite metadata: %s", sqlite_temp)

        # ----------------------------------------------------
        # Import into unified DB
        # ----------------------------------------------------
        logger.info("Wiping existing packages for repo id %s", repo_id)
        self.db.wipe_repo_packages(repo_id)

        try:
            self.db.import_repodb(sqlite_temp, repo_row["name"])
            self.db.update_repo_timestamp(repo_id, datetime.utcnow().isoformat())
        except Exception:
            logger.exception("Failed to import sqlite metadata for repo %s", repo_row["name"])
        finally:
            try:
                os.unlink(sqlite_temp)
            except Exception:
                pass

        # ----------------------------------------------------
        # Optional: link binary → SRPM
        # ----------------------------------------------------
        try:
            self._link_binaries_to_srpm(repo_id)
        except Exception:
            logger.exception("linking binaries to SRPM failed for repo_id %s", repo_id)

        logger.info("Sync complete.")

    # --------------------------------------------------------
    # Step 1: find primary_db sqlite
    # --------------------------------------------------------

    def _find_primary_sqlite_url(self, repomd_bytes: bytes, base_url: str) -> Optional[str]:
        # decode XML
        text = None
        for enc in ("utf-8", "latin1"):
            try:
                text = repomd_bytes.decode(enc)
                break
            except Exception:
                pass
        if not text:
            logger.error("repomd.xml decode failed")
            return None

        try:
            root = ET.fromstring(text)
        except Exception as e:
            logger.error("repomd.xml parse failed: %s", e)
            return None

        ns = {"d": root.tag.split("}")[0].strip("{")} if "}" in root.tag else {"d": ""}

        # STRICT: select <data type="primary_db">
        for data in root.findall("d:data", ns):
            if data.get("type") != "primary_db":
                continue

            loc = data.find("d:location", ns)
            if loc is None:
                continue

            href = loc.get("href")
            if not href:
                continue

            return href if href.startswith("http") else urljoin(base_url.rstrip("/") + "/", href.lstrip("/"))

        return None

    # --------------------------------------------------------
    # Step 2: Download compressed sqlite → decompress → validate
    # --------------------------------------------------------

    def _download_and_extract_sqlite(self, url: str) -> Optional[str]:
        try:
            compressed = self.downloader.download_to_memory(url)
        except Exception as e:
            logger.error("Failed to download sqlite blob: %s", e)
            return None

        data = _decompress_bytes(compressed)

        # SQLite header validation
        if not data.startswith(self.SQLITE_HEADER):
            logger.error("Decompressed file is not SQLite: %s", url)
            return None

        # Write to temp file
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite")
        tmp.close()
        with open(tmp.name, "wb") as f:
            f.write(data)

        return tmp.name

    # --------------------------------------------------------
    # SRPM linking
    # --------------------------------------------------------

    def _link_binaries_to_srpm(self, repo_id: int) -> None:
        src_repo = self.db.get_source_repo(repo_id)
        if not src_repo:
            logger.debug("No source repo linked for repo_id %s", repo_id)
            return

        cur = self.db.conn.execute("SELECT pkgKey, name, rpm_sourcerpm FROM packages WHERE repo_id=?", (repo_id,))

        for row in cur.fetchall():
            pkgKey = row["pkgKey"]
            sourcerpm = row["rpm_sourcerpm"]
            if not sourcerpm:
                continue

            # try find matching SRPM in linked source repo
            s = self.db.conn.execute(
                "SELECT pkgKey FROM packages " "WHERE repo_id=? AND name=? AND arch IN ('src','nosrc') LIMIT 1",
                (src_repo["id"], sourcerpm),
            ).fetchone()

            if s:
                logger.debug("Linked binary pkgKey=%s → SRPM pkgKey=%s", pkgKey, s["pkgKey"])
            else:
                logger.debug("No SRPM match for %s in %s", sourcerpm, src_repo["name"])
