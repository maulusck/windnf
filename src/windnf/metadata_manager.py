# metadata_manager.py
from __future__ import annotations

import bz2
import gzip
import io
import lzma
import os
import re
import tempfile
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from .config import Config
from .db_manager import DbManager
from .downloader import Downloader
from .logger import setup_logger

_logger = setup_logger()


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

    def __init__(self, config: Config, db_manager: DbManager, downloader: Downloader, max_workers: int = 4):
        self.config = config
        self.db = db_manager
        self.downloader = downloader
        self.max_workers = max_workers

    # --------------------------------------------------------
    # Main entry: sync one repo
    # --------------------------------------------------------
    def sync_repo(self, repo_row: Dict[str, Any]) -> None:
        """
        Sync a single repository.
        """
        repo_id = repo_row["id"]
        base_url = repo_row["base_url"]
        repomd_href = repo_row["repomd_url"]
        repomd_url = (
            repomd_href
            if repomd_href.startswith("http")
            else urljoin(base_url.rstrip("/") + "/", repomd_href.lstrip("/"))
        )
        _logger.info("Sync repo '%s' from %s", repo_row["name"], repomd_url)
        try:
            # Download repomd.xml
            repomd_bytes = self.downloader.download_to_memory(repomd_url)
            if re.search(r"techarohq\/anubis", repomd_bytes.decode("utf-8", "ignore"), re.IGNORECASE):
                raise RuntimeError(f"Anubis protection is blocking the download of repo '{repo_row['name']}'")
            # Locate primary sqlite
            sqlite_url = self._find_primary_sqlite_url(repomd_bytes, base_url)
            if not sqlite_url:
                raise RuntimeError("No primary_db found")
            # Download, decompress, validate sqlite
            sqlite_temp = self._download_and_extract_sqlite(sqlite_url)
            if not sqlite_temp:
                raise RuntimeError("Failed to prepare sqlite metadata")
            _logger.info("Using sqlite metadata: %s", sqlite_temp)
            # Import into unified DB
            _logger.info("Wiping existing packages for repo id %s", repo_id)
            self.db.wipe_repo_packages(repo_id)
            self.db.import_repodb(sqlite_temp, repo_row["name"])
            self.db.update_repo_timestamp(repo_id, datetime.utcnow().isoformat())

        except Exception as e:
            print(f"Failed to sync repo '{repo_row['name']}'")
            raise RuntimeError(str(e))
        finally:
            try:
                if "sqlite_temp" in locals() and sqlite_temp:
                    os.unlink(sqlite_temp)
            except Exception:
                pass
        _logger.info("Sync complete for '%s'", repo_row["name"])

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
            _logger.error("repomd.xml decode failed")
            return None

        try:
            root = ET.fromstring(text)
        except Exception as e:
            _logger.error("repomd.xml parse failed: %s", e)
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
            _logger.error("Failed to download sqlite blob: %s", e)
            return None
        data = _decompress_bytes(compressed)
        # SQLite header validation
        if not data.startswith(self.SQLITE_HEADER):
            _logger.error("Decompressed file is not SQLite: %s", url)
            return None
        # Write to temp file
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite")
        tmp.close()
        with open(tmp.name, "wb") as f:
            f.write(data)
        return tmp.name
