# metadata_manager.py
import bz2
import gzip
import io
import lzma
import os
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin

from .config import Config
from .db_manager import DbManager
from .downloader import Downloader
from .logger import setup_logger

_logger = setup_logger()


def extract_namespaces(xml_content: str) -> dict:
    """
    Extract namespaces from the XML string.
    Default namespace ("") is mapped to "default".
    """
    namespaces = {}
    for _, elem in ET.iterparse(io.StringIO(xml_content), events=["start-ns"]):
        prefix, uri = elem
        if prefix == "":
            prefix = "default"
        namespaces[prefix] = uri
    return namespaces


def qname(ns: str, tag: str) -> str:
    """Build a fully-qualified namespace tag."""
    return f"{{{ns}}}{tag}" if ns else tag


class MetadataManager:
    def __init__(self, downloader: Downloader, db: DbManager) -> None:
        self.downloader = downloader
        self.db = db

    # -------------------------------------------------------------------------
    # SYNC REPOSITORY
    # -------------------------------------------------------------------------
    def sync_repo(self, repo_row: dict) -> None:
        repo_id = repo_row["id"]
        name = repo_row["name"]
        base_url = repo_row["base_url"]
        repomd_url = urljoin(base_url, repo_row["repomd_url"])

        _logger.info(f"Syncing repository '{name}' from {repomd_url}")

        repomd_content = self._download_to_memory(repomd_url)
        if not repomd_content:
            _logger.error(f"Failed to download repomd.xml from {repomd_url}")
            return

        repomd_str = repomd_content.decode() if isinstance(repomd_content, bytes) else repomd_content
        repomd_root = ET.fromstring(repomd_str)
        namespaces = extract_namespaces(repomd_str)
        ns_default = namespaces.get("default", "")

        # Find <data type="primary">
        primary_path = None
        for data in repomd_root.findall(qname(ns_default, "data")):
            if data.attrib.get("type") == "primary":
                loc = data.find(qname(ns_default, "location"))
                if loc is not None:
                    primary_path = loc.attrib.get("href")
                    break

        if not primary_path:
            _logger.error("Primary location href not found in repomd.xml")
            return

        primary_url = primary_path if primary_path.startswith("http") else urljoin(base_url, primary_path)

        _logger.info(f"Downloading primary XML from {primary_url}")
        compressed = self._download_to_memory(primary_url)
        if not compressed:
            _logger.error(f"Failed to download primary XML from {primary_url}")
            return

        primary_xml = self._decompress_data(compressed)
        if not primary_xml:
            _logger.error("Failed to decompress primary XML")
            return

        xml_ns = extract_namespaces(primary_xml)
        primary_ns = xml_ns.get("default")
        rpm_ns = xml_ns.get("rpm")

        if not primary_ns or not rpm_ns:
            _logger.error("Missing required XML namespaces 'default' or 'rpm' in primary XML")
            return

        self.db.clear_repo_packages(repo_id)
        self._parse_and_store_packages(repo_id, primary_xml, primary_ns, rpm_ns)
        self.db.update_repo_timestamp(repo_id, datetime.utcnow().isoformat())

        _logger.info(f"Repository '{name}' sync completed.")

    # -------------------------------------------------------------------------
    # DOWNLOAD HELPERS
    # -------------------------------------------------------------------------
    def _download_to_memory(self, url: str) -> Optional[bytes]:
        """
        Download using configured downloader. Returns raw bytes or None on failure.
        """
        if self.downloader.downloader_type.name == "PYTHON":
            try:
                resp = self.downloader.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                _logger.error(f"Downloader error fetching {url}: {e}")
                return None

        # Powershell fallback
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = tmp.name

            self.downloader._download_powershell(url, tmp_path)

            with open(tmp_path, "rb") as f:
                data = f.read()

            try:
                os.remove(tmp_path)
            except OSError as e:
                _logger.warning(f"Failed to delete temp file {tmp_path}: {e}")

            return data

        except Exception as e:
            _logger.error(f"Downloader error fetching {url} via PowerShell: {e}")
            return None

    def _decompress_data(self, data: bytes) -> Optional[str]:
        """
        Attempt gzip → bzip2 → xz → fail.
        """
        for name, func in [
            ("gzip", gzip.decompress),
            ("bzip2", bz2.decompress),
            ("xz", lzma.decompress),
        ]:
            try:
                decompressed = func(data)
                _logger.info(f"Decompressed primary XML using {name}")
                return decompressed.decode("utf-8")
            except Exception:
                continue

        _logger.error("Unsupported compression format for primary XML")
        return None

    # -------------------------------------------------------------------------
    # PARSE PRIMARY XML
    # -------------------------------------------------------------------------
    def _parse_and_store_packages(self, repo_id: int, xml_text: str, primary_ns: str, rpm_ns: str) -> None:
        root = ET.fromstring(xml_text)

        qn = lambda tag: qname(primary_ns, tag)
        qnr = lambda tag: qname(rpm_ns, tag)

        pkgs = root.findall(qn("package"))
        total = len(pkgs)
        _logger.info(f"Total packages in metadata: {total}")

        # Data collected for insertion
        staged = []  # list: (pkg_dict, provides_set, requires_set, weak_requires_set)

        for i, pkg in enumerate(pkgs, start=1):

            # Helpers
            def txt(e, tag):
                el = e.find(qn(tag))
                return el.text if el is not None else None

            def rtxt(e, tag):
                el = e.find(qnr(tag))
                return el.text if el is not None else None

            name = txt(pkg, "name")
            arch = txt(pkg, "arch")
            summary = txt(pkg, "summary")
            description = txt(pkg, "description")
            url = txt(pkg, "url")

            ver_el = pkg.find(qn("version"))
            version, release, epoch = "", "", 0
            if ver_el is not None:
                version = ver_el.get("ver", "")
                release = ver_el.get("rel", "")
                try:
                    epoch = int(ver_el.get("epoch", "0"))
                except ValueError:
                    epoch = 0

            loc_el = pkg.find(qn("location"))
            filepath = loc_el.get("href") if loc_el is not None else None

            if not name or not filepath:
                _logger.warning(f"Skipping malformed package (name={name}, path={filepath})")
                continue

            # Optional metadata
            license_str = vendor_str = group_str = buildhost_str = sourcerpm_str = None
            provides = set()
            requires = set()
            weak_requires = set()
            files = []
            hdr_start = hdr_end = None

            fmt = pkg.find(qn("format"))
            if fmt is not None:
                license_str = rtxt(fmt, "license")
                vendor_str = rtxt(fmt, "vendor")
                group_str = rtxt(fmt, "group")
                buildhost_str = rtxt(fmt, "buildhost")
                sourcerpm_str = rtxt(fmt, "sourcerpm")

                hr = fmt.find(qnr("header-range"))
                if hr is not None:
                    hdr_start = hr.get("start")
                    hdr_end = hr.get("end")

                # Provides
                prov_el = fmt.find(qnr("provides"))
                if prov_el is not None:
                    for entry in prov_el.findall(qnr("entry")):
                        n = entry.get("name")
                        if n:
                            provides.add(n)

                # Requires
                req_el = fmt.find(qnr("requires"))
                if req_el is not None:
                    for entry in req_el.findall(qnr("entry")):
                        n = entry.get("name")
                        if n:
                            requires.add(n)

                # Weak requires
                weak_el = fmt.find(qnr("weakrequires"))
                if weak_el is not None:
                    for entry in weak_el.findall(qnr("entry")):
                        n = entry.get("name")
                        if n:
                            weak_requires.add(n)

                # File list
                files = [f.text for f in fmt.findall(qn("file")) if f.text]

            # Build DB record
            pkg_dict = {
                "name": name,
                "version": version,
                "release": release,
                "epoch": epoch,
                "arch": arch,
                "filepath": filepath,
                "summary": summary,
                "description": description,
                "license": license_str,
                "vendor": vendor_str,
                "group": group_str,
                "buildhost": buildhost_str,
                "sourcerpm": sourcerpm_str,
                "header_range_start": hdr_start,
                "header_range_end": hdr_end,
                "packager": txt(pkg, "packager"),
                "url": url,
                "files": files,
            }

            staged.append((pkg_dict, provides, requires, weak_requires))

            if i % 500 == 0 or i == total:
                _logger.info(f"Parsed {i}/{total} packages ({i/total*100:.2f}%)")

        # Insert packages
        pkg_ids = self.db.add_packages(repo_id, [p[0] for p in staged])

        # Insert dependencies
        for idx, pkg_id in enumerate(pkg_ids):
            provides, requires, weak = staged[idx][1:]
            if provides:
                self.db.add_provides(pkg_id, provides)
            if requires:
                self.db.add_requires(pkg_id, requires, is_weak=False)
            if weak:
                self.db.add_requires(pkg_id, weak, is_weak=True)

        _logger.info(f"Inserted {len(pkg_ids)} packages into the database.")
