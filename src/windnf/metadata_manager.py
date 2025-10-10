import bz2
import gzip
import io
import lzma
import xml.etree.ElementTree as ET
from typing import Optional
from urllib.parse import urljoin

from .config import Config
from .db_manager import DbManager
from .downloader import Downloader
from .logger import setup_logger

_logger = setup_logger()


def extract_namespaces(xml_content: str) -> dict:
    namespaces = {}
    for event, elem in ET.iterparse(io.StringIO(xml_content), events=["start-ns"]):
        prefix, uri = elem
        if prefix == "":
            prefix = "default"
        namespaces[prefix] = uri
    return namespaces


def qname(ns: str, tag: str) -> str:
    return f"{{{ns}}}{tag}" if ns else tag


class MetadataManager:
    def __init__(self, downloader: Downloader, db: DbManager) -> None:
        self.downloader = downloader
        self.db = db

    def sync_repo(self, repo_row) -> None:
        repo_id = repo_row["id"]
        name = repo_row["name"]
        base_url = repo_row["base_url"]
        repomd_url = repo_row["repomd_url"]

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
        self._parse_and_store_packages(repo_id, primary_xml_str, primary_ns, rpm_ns)

        from datetime import datetime

        self.db.update_repo_timestamp(repo_id, datetime.utcnow().isoformat())
        _logger.info(f"Repository '{name}' sync completed.")

    def _download_to_memory(self, url: str) -> Optional[bytes]:
        import tempfile

        if self.downloader.downloader_type.name == "PYTHON":
            try:
                resp = self.downloader.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                _logger.error(f"Downloader error fetching {url}: {e}")
                return None
        else:
            try:
                with tempfile.NamedTemporaryFile(delete=True) as tmpfile:
                    self.downloader._download_powershell(url, tmpfile.name)
                    tmpfile.flush()
                    tmpfile.seek(0)
                    return tmpfile.read()
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

    def _parse_and_store_packages(self, repo_id: int, xml_content: str, primary_ns: str, rpm_ns: str) -> None:
        root = ET.fromstring(xml_content)

        def qn(tag):
            return qname(primary_ns, tag)

        def qnr(tag):
            return qname(rpm_ns, tag)

        packages_total = len(root.findall(qn("package")))
        _logger.info(f"Total packages to parse: {packages_total}")

        count = 0
        for pkg_elem in root.findall(qn("package")):
            name_elem = pkg_elem.find(qn("name"))
            version_elem = pkg_elem.find(qn("version"))
            location_elem = pkg_elem.find(qn("location"))
            arch_elem = pkg_elem.find(qn("arch"))

            missing_fields = []
            if name_elem is None:
                missing_fields.append("name")
            if version_elem is None:
                missing_fields.append("version")
            if location_elem is None:
                missing_fields.append("location")
            if missing_fields:
                _logger.warning(
                    f"Package missing required fields {missing_fields}; skipping. Element:\n{ET.tostring(pkg_elem, encoding='unicode')}"
                )
                continue

            name = name_elem.text or ""
            version = version_elem.get("ver", "")
            release = version_elem.get("rel", "")
            try:
                epoch = int(version_elem.get("epoch", "0"))
            except ValueError:
                epoch = 0
            arch = arch_elem.text if arch_elem is not None else ""
            filepath = location_elem.get("href", "")

            if not name or not filepath:
                _logger.warning(
                    f"Package missing name or location value; skipping. name={name!r} filepath={filepath!r}"
                )
                continue

            pkg_id = self.db.add_package(repo_id, name, version, release, epoch, arch, filepath)

            fmt_elem = pkg_elem.find(qn("format"))

            provides_set = set()
            requires_set = set()
            weak_requires_set = set()
            if fmt_elem is not None:
                provides_elem = fmt_elem.find(qnr("provides"))
                if provides_elem is not None:
                    for entry in provides_elem.findall(qnr("entry")):
                        pname = entry.get("name")
                        if pname:
                            provides_set.add(pname)

                requires_elem = fmt_elem.find(qnr("requires"))
                if requires_elem is not None:
                    for entry in requires_elem.findall(qnr("entry")):
                        rname = entry.get("name")
                        if rname:
                            requires_set.add(rname)

                weak_requires_elem = fmt_elem.find(qnr("weakrequires"))
                if weak_requires_elem is not None:
                    for entry in weak_requires_elem.findall(qnr("entry")):
                        wname = entry.get("name")
                        if wname:
                            weak_requires_set.add(wname)

            self.db.add_provides(pkg_id, provides_set)
            self.db.add_requires(pkg_id, requires_set, is_weak=False)
            self.db.add_requires(pkg_id, weak_requires_set, is_weak=True)

            count += 1
            if count % 500 == 0 or count == packages_total:
                percent = (count / packages_total) * 100
                _logger.info(f"{percent:.2f}% parsed and stored {count} packages out of {packages_total}")

        _logger.info(f"Completed parsing and storing {count} packages.")
