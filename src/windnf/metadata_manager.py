from __future__ import annotations

import gzip
import hashlib
import bz2
import lzma
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin

# Security: Safe XML parsing
try:
    import defusedxml.ElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

from .config import Config
from .db_manager import DbManager
from .downloader import Downloader
from .logger import setup_logger

_logger = setup_logger()


class MetadataManager:
    """
    RPM metadata syncing with high-performance streaming and security checks.
    
    Pipeline:
      1. Download repomd.xml (small, in-memory)
      2. Parse checksums for primary_db
      3. Stream-download compressed DB to disk + Verify Checksum (On-the-fly)
      4. Stream-decompress to SQLite temp file
      5. Batch import to local DB
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
        repo_id = repo_row["id"]
        base_url = repo_row["base_url"]
        repomd_href = repo_row["repomd_url"]

        # 1. Resolve repomd.xml URL
        repomd_url = (
            repomd_href
            if repomd_href.startswith("http")
            else urljoin(base_url.rstrip("/") + "/", repomd_href.lstrip("/"))
        )

        _logger.info("Syncing repo '%s'...", repo_row["name"])

        # 2. Fetch repomd.xml
        try:
            # repomd.xml is small, safe to keep in RAM
            repomd_bytes = self.downloader.download_to_memory(repomd_url)
        except Exception as e:
            _logger.error("Failed to download repomd.xml for %s: %s", repo_row["name"], e)
            return

        # 3. Parse Metadata Location & Checksum
        target_meta = self._find_primary_sqlite_info(repomd_bytes, base_url)
        if not target_meta:
            _logger.error("No valid 'primary_db' found in repomd.xml for '%s'", repo_row["name"])
            return

        sqlite_url, expected_checksum, checksum_type = target_meta
        _logger.debug("Found primary_db: %s (%s: %s)", sqlite_url, checksum_type, expected_checksum)

        # 4. Stream Download + Verify + Decompress
        sqlite_temp_path = self._process_metadata_file(sqlite_url, expected_checksum, checksum_type)
        if not sqlite_temp_path:
            _logger.error("Failed to process metadata for '%s'", repo_row["name"])
            return

        # 5. Import into Unified DB
        _logger.info("Importing metadata for '%s' (this may take a moment)...", repo_row["name"])
        self.db.wipe_repo_packages(repo_id)

        try:
            self.db.import_repodb(sqlite_temp_path, repo_row["name"])
            self.db.update_repo_timestamp(repo_id, datetime.utcnow().isoformat())
        except Exception:
            _logger.exception("DB Import failed for %s", repo_row["name"])
        finally:
            # Cleanup decompressed temp file
            try:
                os.unlink(sqlite_temp_path)
            except Exception:
                pass

        _logger.info("Sync complete for '%s'.", repo_row["name"])

    # --------------------------------------------------------
    # Logic: Parse repomd.xml
    # --------------------------------------------------------

    def _find_primary_sqlite_info(self, repomd_bytes: bytes, base_url: str) -> Optional[Tuple[str, str, str]]:
        """
        Returns (url, checksum_value, checksum_type) for the primary_db.
        """
        try:
            root = ET.fromstring(repomd_bytes)
        except Exception as e:
            _logger.error("XML parse error: %s", e)
            return None

        # Handle namespaces (yum standard vs others)
        # Usually: <repomd xmlns="http://linux.duke.edu/metadata/repo">
        ns = {}
        if "}" in root.tag:
            ns_url = root.tag.split("}")[0].strip("{")
            ns = {"d": ns_url}

        # Look for <data type="primary_db">
        for data in root.findall("d:data" if ns else "data", ns):
            if data.get("type") != "primary_db":
                continue

            # Get Location
            loc = data.find("d:location" if ns else "location", ns)
            if loc is None: 
                continue
            href = loc.get("href")
            
            # Get Checksum
            csum_node = data.find("d:checksum" if ns else "checksum", ns)
            if csum_node is None:
                continue
            
            csum_val = csum_node.text
            csum_type = csum_node.get("type")

            if not href or not csum_val or not csum_type:
                continue

            # Resolve absolute URL
            full_url = href if href.startswith("http") else urljoin(base_url.rstrip("/") + "/", href.lstrip("/"))
            return (full_url, csum_val, csum_type)

        return None

    # --------------------------------------------------------
    # Logic: Stream Download -> Checksum -> Decompress
    # --------------------------------------------------------

    def _process_metadata_file(self, url: str, expected_csum: str, csum_type: str) -> Optional[str]:
        """
        Downloads compressed file to temp, verifies hash, decompresses to new temp.
        Returns path to DECOMPRESSED sqlite file.
        """
        # 1. Create temp file for COMPRESSED data
        tf_compressed = tempfile.NamedTemporaryFile(delete=False, prefix="windnf_dl_")
        tf_compressed.close()
        
        try:
            # 2. Download with on-the-fly hashing
            _logger.debug("Downloading compressed metadata...")
            if not self._download_and_verify(url, tf_compressed.name, expected_csum, csum_type):
                return None
            
            # 3. Decompress to new temp file
            tf_sqlite = tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite", prefix="windnf_db_")
            tf_sqlite.close()
            
            _logger.debug("Decompressing metadata...")
            if self._decompress_stream(tf_compressed.name, tf_sqlite.name):
                return tf_sqlite.name
            else:
                os.unlink(tf_sqlite.name)
                return None
                
        finally:
            # Always clean up the compressed artifact
            if os.path.exists(tf_compressed.name):
                os.unlink(tf_compressed.name)

    def _download_and_verify(self, url: str, dest_path: str, expected: str, algo: str) -> bool:
        """
        Stream download chunks, update hash, write to disk.
        """
        try:
            # Validate hash algorithm support
            if algo not in hashlib.algorithms_available:
                _logger.error("Unsupported checksum algorithm: %s", algo)
                return False
            
            hasher = hashlib.new(algo)
            
            # Use downloader's session to get stream
            if not self.downloader.session:
                self.downloader._init_session()

            with self.downloader.session.get(url, stream=True, timeout=self.downloader.timeout) as resp:
                resp.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536): # 64k chunks
                        if chunk:
                            hasher.update(chunk)
                            f.write(chunk)
            
            # Verify
            calculated = hasher.hexdigest()
            if calculated != expected:
                _logger.critical("Checksum Mismatch! Expected %s, got %s", expected, calculated)
                return False
            
            return True

        except Exception as e:
            _logger.error("Download/Verification failed: %s", e)
            return False

    def _decompress_stream(self, src_path: str, dest_path: str) -> bool:
        """
        Stream decompress (gzip/bz2/xz) -> destination.
        RAM usage is constant (buffer size).
        """
        try:
            # Detect compression by file signature (magic bytes)
            with open(src_path, "rb") as f:
                magic = f.read(6)
            
            opener = None
            if magic.startswith(b"\x1f\x8b"):
                opener = gzip.open
            elif magic.startswith(b"BZh"):
                opener = bz2.open
            elif magic.startswith(b"\xfd7zXZ"):
                opener = lzma.open
            else:
                _logger.error("Unknown compression type (Magic: %s)", magic.hex())
                return False

            # Stream Decompression
            with opener(src_path, "rb") as f_in, open(dest_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out) # Highly optimized chunked copy
                
            # Validate SQLite Header (sanity check)
            with open(dest_path, "rb") as f_check:
                header = f_check.read(16)
                if not header.startswith(self.SQLITE_HEADER):
                    _logger.error("Decompressed file is not a valid SQLite DB")
                    return False
            
            return True

        except Exception as e:
            _logger.error("Decompression failed: %s", e)
            return False