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
    Extract namespaces from the XML string, returning a prefix->URI dict.
    If default namespace is found with empty prefix, it is mapped to 'default'.
    """
    namespaces = {}
    for event, elem in ET.iterparse(io.StringIO(xml_content), events=["start-ns"]):
        prefix, uri = elem
        if prefix == "":
            prefix = "default"
        namespaces[prefix] = uri
    return namespaces


def qname(ns: str, tag: str) -> str:
    """Format a tag name with namespace URI if given."""
    return f"{{{ns}}}{tag}" if ns else tag


class MetadataManager:
    def __init__(self, downloader: Downloader, db: DbManager) -> None:
        self.downloader = downloader
        self.db = db

    def sync_repo(self, repo_row) -> None:
        repo_id = repo_row["id"]
        name = repo_row["name"]
        base_url = repo_row["base_url"]
        repomd_url = urljoin(base_url, repo_row["repomd_url"])

        _logger.info(f"Syncing repository '{name}' from {repomd_url}")

        repomd_content = self._download_to_memory(repomd_url)
        if not repomd_content:
            _logger.error(f"Failed to download repomd.xml from {repomd_url}")
            return

        repomd_str = repomd_content.decode("utf-8") if isinstance(repomd_content, bytes) else repomd_content
        repomd_root = ET.fromstring(repomd_str)
        namespaces = extract_namespaces(repomd_str)
        repo_ns = namespaces.get("default", "")

        primary_path = None
        for data in repomd_root.findall(qname(repo_ns, "data")):
            if data.attrib.get("type") == "primary":
                location = data.find(qname(repo_ns, "location"))
                if location is not None:
                    primary_path = location.attrib.get("href")
                    if primary_path:
                        break

        if not primary_path:
            _logger.error("Primary location href not found in repomd.xml")
            return

        primary_url = primary_path if primary_path.startswith("http") else urljoin(base_url, primary_path)
        _logger.info(f"Downloading primary XML from {primary_url}")

        compressed_primary = self._download_to_memory(primary_url)
        if not compressed_primary:
            _logger.error(f"Failed to download primary XML from {primary_url}")
            return

        primary_xml_content = self._decompress_data(compressed_primary)
        if not primary_xml_content:
            _logger.error("Failed to decompress primary XML data")
            return

        primary_xml_str = (
            primary_xml_content if isinstance(primary_xml_content, str) else primary_xml_content.decode("utf-8")
        )

        xml_namespaces = extract_namespaces(primary_xml_str)
        primary_ns = xml_namespaces.get("default")
        rpm_ns = xml_namespaces.get("rpm")

        if not primary_ns or not rpm_ns:
            _logger.error("Missing required XML namespaces 'default' or 'rpm' in primary XML")
            return

        self.db.clear_repo_packages(repo_id)
        self._parse_and_store_packages_bulk(repo_id, primary_xml_str, primary_ns, rpm_ns)

        self.db.update_repo_timestamp(repo_id, datetime.utcnow().isoformat())
        _logger.info(f"Repository '{name}' sync completed.")

    def _download_to_memory(self, url: str) -> Optional[bytes]:
        if self.downloader.downloader_type.name == "PYTHON":
            try:
                resp = self.downloader.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                _logger.error(f"Downloader error fetching {url}: {e}")
                return None
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_path = tmp_file.name
            self.downloader._download_powershell(url, tmp_path)
            with open(tmp_path, "rb") as f:
                content = f.read()
            try:
                os.remove(tmp_path)
            except OSError as e:
                _logger.warning(f"Failed to delete temp file {tmp_path}: {e}")
            return content
        except Exception as e:
            _logger.error(f"Downloader error fetching {url} via PowerShell: {e}")
            return None

    def _decompress_data(self, data: bytes) -> Optional[str]:
        decompressors = [
            ("gzip", gzip.decompress),
            ("bzip2", bz2.decompress),
            ("xz", lzma.decompress),
        ]
        for name, decompressor in decompressors:
            try:
                decompressed = decompressor(data)
                _logger.info(f"Decompressed primary XML with {name}")
                return decompressed.decode("utf-8")
            except Exception:
                continue
        _logger.error("Unsupported compression format for primary XML")
        return None

    def _parse_and_store_packages_bulk(self, repo_id: int, xml_content: str, primary_ns: str, rpm_ns: str) -> None:
        root = ET.fromstring(xml_content)

        def qn(tag):
            return qname(primary_ns, tag)

        def qnr(tag):
            return qname(rpm_ns, tag)

        total_packages = len(root.findall(qn("package")))
        _logger.info(f"Total packages to parse: {total_packages}")

        packages_data: List[Dict] = []
        count = 0

        for pkg_elem in root.findall(qn("package")):

            # Utility functions for property extraction
            def get_text(elem, tag):
                el = elem.find(qn(tag))
                return el.text if el is not None else None

            def get_rpm_text(elem, tag):
                el = elem.find(qnr(tag))
                return el.text if el is not None else None

            def get_attrib(elem, *attrs):
                for attr in attrs:
                    val = elem.get(attr)
                    if val:
                        return val
                return None

            # Basic properties with fallbacks
            name = get_text(pkg_elem, "name")
            arch = get_text(pkg_elem, "arch")
            packager = get_text(pkg_elem, "packager")
            summary = get_text(pkg_elem, "summary")
            description = get_text(pkg_elem, "description")
            url = get_text(pkg_elem, "url")

            # Version info
            version_elem = pkg_elem.find(qn("version"))
            version = release = ""
            epoch = 0
            if version_elem is not None:
                version = version_elem.get("ver", "")
                release = version_elem.get("rel", "")
                try:
                    epoch = int(version_elem.get("epoch", "0"))
                except ValueError:
                    epoch = 0

            # Location href
            location_elem = pkg_elem.find(qn("location"))
            filepath = location_elem.get("href") if location_elem is not None else None

            if not name or not filepath:
                _logger.warning(f"Skipping package missing name or location. Name: {name}, Path: {filepath}")
                continue

            # Initialize optional properties
            license_str = vendor_str = group_str = buildhost_str = sourcerpm_str = None
            header_range_start = header_range_end = None
            provides_set = set()
            requires_set = set()
            weak_requires_set = set()
            files_list = []

            # Parse optional <format> element
            format_elem = pkg_elem.find(qn("format"))
            if format_elem is not None:
                license_str = get_rpm_text(format_elem, "license")
                vendor_str = get_rpm_text(format_elem, "vendor")
                group_str = get_rpm_text(format_elem, "group")
                buildhost_str = get_rpm_text(format_elem, "buildhost")
                sourcerpm_str = get_rpm_text(format_elem, "sourcerpm")

                # Header range
                header_range_elem = format_elem.find(qnr("header-range"))
                if header_range_elem is not None:
                    header_range_start = header_range_elem.get("start")
                    header_range_end = header_range_elem.get("end")

                # Provides
                provides_elem = format_elem.find(qnr("provides"))
                if provides_elem is not None:
                    for entry in provides_elem.findall(qnr("entry")):
                        pname = entry.get("name")
                        if pname:
                            provides_set.add(pname)

                # Requires
                requires_elem = format_elem.find(qnr("requires"))
                if requires_elem is not None:
                    for entry in requires_elem.findall(qnr("entry")):
                        rname = entry.get("name")
                        if rname:
                            requires_set.add(rname)

                # Weak Requires
                weak_requires_elem = format_elem.find(qnr("weakrequires"))
                if weak_requires_elem is not None:
                    for entry in weak_requires_elem.findall(qnr("entry")):
                        wname = entry.get("name")
                        if wname:
                            weak_requires_set.add(wname)

                # Files list
                files_list = [f.text for f in format_elem.findall(qn("file")) if f.text]

            # Construct package record
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
                "header_range_start": header_range_start,
                "header_range_end": header_range_end,
                "packager": get_text(pkg_elem, "packager"),
                "url": url,
                "files": files_list,
            }

            packages_data.append((pkg_dict, provides_set, requires_set, weak_requires_set))
            count += 1

            if count % 500 == 0 or count == total_packages:
                percent = (count / total_packages) * 100
                _logger.info(f"{percent:.2f}% parsed: {count}/{total_packages}")

        # Save to database
        pkg_ids = self.db.add_packages_bulk(repo_id, [p[0] for p in packages_data])

        # Insert dependencies
        for i, pkg_id in enumerate(pkg_ids):
            provides, requires, weak_requires = packages_data[i][1], packages_data[i][2], packages_data[i][3]
            if provides:
                self.db.add_provides(pkg_id, provides)
            if requires:
                self.db.add_requires(pkg_id, requires, is_weak=False)
            if weak_requires:
                self.db.add_requires(pkg_id, weak_requires, is_weak=True)

        _logger.info(f"Finished processing {len(pkg_ids)} packages.")
