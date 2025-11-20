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
    return any(fnmatch.fnmatch(name, pat) for pat in patterns)


def confirm(prompt: str) -> bool:
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

    if all_:
        if not force and not confirm("Delete ALL repositories and their packages?"):
            _logger.info("Operation canceled.")
            return
        target_names = [r["name"] for r in repos]
    else:
        target_names = []
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

    matched_pkgs = db.search_packages(patterns, repo_names=repoids) or []
    if not matched_pkgs:
        print("No packages matched your search.")
        return

    if not showduplicates:
        newest = {}
        for pkg in matched_pkgs:
            key = (pkg["name"], pkg["arch"])
            version_tuple = (pkg.get("epoch", 0), pkg.get("version", ""), pkg.get("release", ""))
            if key not in newest or version_tuple > (
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
# Dependency resolution (split)
# -------------------------------


def resolve_calc(
    packages: List[str],
    repoids: Optional[List[str]] = None,
    recursive: bool = False,
    weakdeps: bool = False,
    arch: Optional[str] = None,
) -> List[dict]:
    """
    Pure dependency calculation.
    Returns a list of package dicts.
    """
    initial = db.search_packages(packages, repo_names=repoids)
    if not initial:
        return []

    all_packages = db.get_all_packages()
    provides_map = db.get_provides_map()
    requires_map = db.get_requires_map()

    resolved_ids: Set[int] = set()
    to_process: List[int] = [p["id"] for p in initial]

    while to_process:
        pid = to_process.pop()
        if pid in resolved_ids:
            continue
        resolved_ids.add(pid)

        if recursive:
            for req_name, is_weak in requires_map.get(pid, []):
                if is_weak and not weakdeps:
                    continue

                cand_ids = provides_map.get(req_name, set())
                if not cand_ids:
                    fallback = next((p for p in all_packages.values() if p["name"] == req_name), None)
                    if fallback:
                        cand_ids = {fallback["id"]}

                for cid in cand_ids:
                    if cid not in resolved_ids:
                        to_process.append(cid)

    pkgs = [all_packages[i] for i in resolved_ids if i in all_packages]

    if arch:
        pkgs = [p for p in pkgs if p.get("arch") == arch]

    return pkgs


def resolve_print(pkgs: List[dict]):
    """
    Print the resolved dependency tree.
    """
    if not pkgs:
        print("No matching packages.")
        return

    seen = set()
    for idx, pkg in enumerate(pkgs):
        if pkg["id"] in seen:
            continue
        seen.add(pkg["id"])
        branch = "└─" if idx == len(pkgs) - 1 else "├─"
        print(f"{branch} {pkg['name']}-{pkg['version']}-{pkg['release']} ({pkg['arch']})")


def resolve(
    packages: List[str],
    repoids: Optional[List[str]] = None,
    recursive: bool = False,
    weakdeps: bool = False,
    arch: Optional[str] = None,
):
    """
    CLI entry: resolve command.
    """
    pkgs = resolve_calc(packages, repoids, recursive, weakdeps, arch)
    resolve_print(pkgs)


# -------------------------------
# Download packages (uses resolve_calc)
# -------------------------------
def download(
    packages: List[str],
    repoids: Optional[List[str]] = None,
    downloaddir: Optional[str] = None,
    resolve: bool = False,
    recurse: bool = False,
    source: bool = False,
    urls: bool = False,
    arch: Optional[str] = None,
):
    """
    Download packages using repo_id and relative filepath.
    """
    downloaddir = Path(downloaddir or config.download_path)
    downloaddir.mkdir(parents=True, exist_ok=True)
    _logger.info(f"Using download directory: {downloaddir}")

    # Step 1: initial match
    all_pkgs = db.search_packages(["*"], repo_names=repoids) or []
    matched = [p for p in all_pkgs if match_patterns(p["name"], packages)]
    if not matched:
        print("No packages matched the download request.")
        return

    # Step 2: resolve dependencies if requested
    if resolve or recurse:
        matched = resolve_calc(
            [p["name"] for p in matched],
            repoids=repoids,
            recursive=recurse,
            weakdeps=False,
            arch=arch,
        )

    # Filter out packages missing repo_id or filepath
    matched = [p for p in matched if p.get("repo_id") and p.get("filepath")]
    if not matched:
        print("No downloadable packages after dependency resolution.")
        return

    # Step 3: convert to SRPMs if requested
    if source:
        srpms = []
        for pkg in matched:
            src_name = pkg.get("sourcerpm")
            if not src_name:
                continue
            spkg = db.get_package_info_by_repoid(pkg["repo_id"], src_name)
            if spkg:
                srpms.append(spkg)
        matched = srpms
        if not matched:
            print("No packages after SRPM conversion.")
            return

    # Step 4: lookup repositories
    repo_map = {r["id"]: r for r in db.get_repositories()}

    # Step 5: URLs only
    if urls:
        for pkg in matched:
            repo = repo_map.get(pkg["repo_id"])
            if not repo:
                _logger.warning(f"Skipping {pkg['name']} (repo not found)")
                continue
            url = urljoin(repo["base_url"].rstrip("/") + "/", pkg["filepath"].lstrip("/"))
            print(url)
        return

    # Step 6: actual downloads
    downloaded = 0
    for pkg in matched:
        repo = repo_map.get(pkg["repo_id"])
        if not repo:
            _logger.warning(f"Skipping {pkg['name']} (repo not found)")
            continue

        url = urljoin(repo["base_url"].rstrip("/") + "/", pkg["filepath"].lstrip("/"))
        dest_file = downloaddir / Path(pkg["filepath"]).name

        _logger.info(f"Downloading {pkg['name']}-{pkg['version']}-{pkg['release']} ({pkg.get('arch')})…")
        try:
            downloader.download(url, dest_file)
            downloaded += 1
        except Exception as e:
            _logger.error(f"Failed to download {pkg['name']}: {e}")

    _logger.info(f"Downloaded {downloaded} packages.")
