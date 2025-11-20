# operations.py
import fnmatch
from pathlib import Path
from typing import List, Optional, Set
from urllib.parse import urljoin

from .config import Config
from .db_manager import DbManager
from .downloader import Downloader
from .logger import setup_logger
from .metadata_manager import MetadataManager

_logger = setup_logger()

config = Config()
db = DbManager(config.db_path)
downloader = Downloader(config)
metadata_mgr = MetadataManager(downloader, db)


# -------------------------------
# Helper: pattern matching
# -------------------------------
def match_patterns(name: str, patterns: List[str]) -> bool:
    """Check if the name matches any of the given patterns."""
    return any(fnmatch.fnmatch(name, pat) for pat in patterns)


def confirm(prompt: str) -> bool:
    """Prompt the user for confirmation."""
    try:
        return input(f"{prompt} [y/N]: ").strip().lower() == "y"
    except EOFError:
        return False


# -------------------------------
# Repository commands
# -------------------------------
def repoadd(name: str, baseurl: str, repomd: str):
    db.add_repository(name, baseurl, repomd)
    _logger.info(f"Added repository '{name}'")


def repolist():
    repos = db.get_repositories()
    if not repos:
        print("No repositories configured.")
        return
    for r in repos:
        print(f"{r['name']:20} {r['base_url']} (repomd: {r['repomd_url']})")


def _get_target_repos(names: Optional[List[str]] = None, all_: bool = False):
    if all_:
        return db.get_repositories()
    if not names:
        return []
    return [db.get_repo_by_name(n) for n in names if db.get_repo_by_name(n)]


def reposync(names: Optional[List[str]] = None, all_: bool = False):
    repos = _get_target_repos(names, all_)
    for repo in repos:
        _logger.info(f"Syncing repository '{repo['name']}' …")
        metadata_mgr.sync_repo(repo)


def repodel(names: Optional[List[str]] = None, force: bool = False, all_: bool = False):
    repos = _get_target_repos(names, all_)
    if not repos:
        _logger.info("No repositories to delete.")
        return

    target_names = []
    if all_:
        if not force and not confirm("Delete ALL repositories and their packages?"):
            _logger.info("Operation canceled.")
            return
        target_names = [r["name"] for r in repos]
    else:
        for repo in repos:
            if force or confirm(f"Delete repository '{repo['name']}' and its packages?"):
                target_names.append(repo["name"])

    for name in target_names:
        db.delete_repository(name)


# -------------------------------
# Search packages
# -------------------------------
def search(patterns: List[str], repoids: Optional[List[str]] = None, showduplicates: bool = False):
    if not patterns:
        print("No search patterns provided.")
        return

    matched_pkgs = [dict(p) for p in db.search_packages(patterns, repo_names=repoids) or []]
    if not matched_pkgs:
        print("No packages matched your search.")
        return

    if not showduplicates:
        newest = {}
        for pkg in matched_pkgs:
            key = (pkg["name"], pkg["arch"])
            current_version = (pkg.get("epoch", 0), pkg.get("version", ""), pkg.get("release", ""))
            if key not in newest or current_version > (
                newest[key].get("epoch", 0),
                newest[key].get("version", ""),
                newest[key].get("release", ""),
            ):
                newest[key] = pkg
        matched_pkgs = list(newest.values())

    matched_pkgs.sort(key=lambda p: (p["name"].lower(), -p.get("epoch", 0), p.get("version", ""), p.get("release", "")))

    for pkg in matched_pkgs:
        print(f"{pkg['name']:<40} {pkg['version']}-{pkg['release']:<20} {pkg['arch']:<10} {pkg['repo_name']}")
        if pkg.get("summary"):
            print(f"    {pkg['summary']}")


# -------------------------------
# Resolve dependencies
# -------------------------------
def resolve(
    packages: List[str],
    repoids: Optional[List[str]] = None,
    recursive: bool = False,
    weakdeps: bool = False,
    arch: Optional[str] = None,
):
    initial = db.search_packages(packages, repo_names=repoids)
    if not initial:
        print("No matching packages.")
        return

    all_packages = db.get_all_packages()
    provides_map = db.get_provides_map()
    requires_map = db.get_requires_map()

    resolved_ids: Set[int] = set()
    to_process: List[int] = [p["id"] for p in initial]

    while to_process:
        current_id = to_process.pop()
        if current_id in resolved_ids:
            continue
        resolved_ids.add(current_id)

        if recursive:
            for req_name, is_weak in requires_map.get(current_id, []):
                if not weakdeps and is_weak:
                    continue
                candidate_ids = provides_map.get(req_name, set())
                if not candidate_ids:
                    pkg_row = next((p for p in all_packages.values() if p["name"] == req_name), None)
                    if pkg_row:
                        candidate_ids.add(pkg_row["id"])
                to_process.extend(cid for cid in candidate_ids if cid not in resolved_ids)

    resolved_pkgs = [all_packages[pid] for pid in resolved_ids if pid in all_packages]
    if arch:
        resolved_pkgs = [p for p in resolved_pkgs if p["arch"] == arch]

    if not resolved_pkgs:
        print("No matching packages.")
        return

    seen = set()
    for idx, pkg in enumerate(resolved_pkgs):
        if pkg["id"] in seen:
            continue
        seen.add(pkg["id"])
        branch = "└─" if idx == len(resolved_pkgs) - 1 else "├─"
        print(f"{branch} {pkg['name']}-{pkg['version']}-{pkg['release']} ({pkg['arch']})")


# -------------------------------
# Download packages
# -------------------------------
def download(
    packages: list[str],
    repoids: list[str] | None = None,
    downloaddir: str | None = None,
    resolve: bool = False,
    recurse: bool = False,
    source: bool = False,
    urls: bool = False,
    arch: str | None = None,
):
    """
    Download packages matching patterns from repositories.

    Args:
        packages: list of package names or patterns
        repoids: list of repository names to filter
        downloaddir: directory to save downloaded packages (defaults to Config.download_path)
        resolve: include dependencies
        recurse: recursively resolve dependencies (implies resolve)
        source: download source RPMs instead of binaries
        urls: print URLs only, do not download
        arch: filter packages by architecture
    """
    # Use default from Config if downloaddir not provided
    downloaddir = Path(downloaddir or config.download_path)
    downloaddir.mkdir(parents=True, exist_ok=True)
    _logger.info(f"Using download directory: {downloaddir}")

    # Step 1: fetch packages from DB
    all_pkgs = db.search_packages(["*"], repo_names=repoids) or []
    matched_pkgs = [p for p in all_pkgs if match_patterns(p["name"], packages)]
    if not matched_pkgs:
        print("No packages matched the download request.")
        return

    # Step 2: resolve dependencies if requested
    if resolve or recurse:
        recursive = recurse
        weakdeps = False
        all_packages = db.get_all_packages()
        provides_map = db.get_provides_map()
        requires_map = db.get_requires_map()

        resolved_ids: set[int] = set(p["id"] for p in matched_pkgs)
        to_process: list[int] = [p["id"] for p in matched_pkgs]

        while to_process:
            pkg_id = to_process.pop()
            if pkg_id in resolved_ids:
                continue
            resolved_ids.add(pkg_id)

            if recursive:
                for req_name, is_weak in requires_map.get(pkg_id, []):
                    if not weakdeps and is_weak:
                        continue
                    candidate_ids = provides_map.get(req_name, set())
                    if not candidate_ids:
                        pkg_row = next((p for p in all_packages.values() if p["name"] == req_name), None)
                        if pkg_row:
                            candidate_ids.add(pkg_row["id"])
                    to_process.extend(cid for cid in candidate_ids if cid not in resolved_ids)

        matched_pkgs = [all_packages[pid] for pid in resolved_ids if pid in all_packages]

    # Step 3: convert to source packages if requested
    if source:
        matched_pkgs = [
            src_info
            for pkg in matched_pkgs
            if (src_info := db.get_package_info(pkg["repo_name"], pkg.get("sourcerpm") or ""))
        ]

    # Step 4: filter by architecture
    if arch:
        matched_pkgs = [p for p in matched_pkgs if p.get("arch") == arch]

    if not matched_pkgs:
        print("No packages to download after filtering.")
        return

    # Step 5: print URLs only
    if urls:
        repo_map = {r["name"]: r for r in db.get_repositories()}
        for pkg in matched_pkgs:
            url = pkg.get("url") or urljoin(
                repo_map[pkg["repo_name"]]["base_url"].rstrip("/") + "/", pkg["filepath"].lstrip("/")
            )
            if url:
                print(url)
        return

    # Step 6: perform downloads
    repo_map = {r["name"]: r for r in db.get_repositories()}
    for pkg in matched_pkgs:
        repo = repo_map.get(pkg["repo_name"])
        if not repo:
            _logger.warning(f"Skipping {pkg['name']} (repo not found)")
            continue

        url = pkg.get("url") or urljoin(repo["base_url"].rstrip("/") + "/", pkg["filepath"].lstrip("/"))
        dest_file = downloaddir / Path(pkg["filepath"]).name
        _logger.info(f"Downloading {pkg['name']}-{pkg['version']}-{pkg['release']} ({pkg.get('arch')}) …")
        try:
            downloader.download(url, dest_file)
        except Exception as e:
            _logger.error(f"Failed to download {pkg['name']}: {e}")

    _logger.info(f"Downloaded {len(matched_pkgs)} packages.")
