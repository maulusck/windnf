# operations.py
import fnmatch
import sys
from typing import List, Optional

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
    for pat in patterns:
        if "*" not in pat and "?" not in pat:
            if pat in name:
                return True
        else:
            if fnmatch.fnmatch(name, pat):
                return True
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


def reposync(names: Optional[List[str]] = None, all_: bool = False):
    if all_:
        repos = db.get_repositories()
    else:
        repos = [db.get_repo_by_name(n) for n in (names or []) if db.get_repo_by_name(n)]
    for repo in repos:
        _logger.info(f"Syncing repository '{repo['name']}' …")
        metadata_mgr.sync_repo(repo)


def repodel(name: Optional[str] = None, force: bool = False, all_: bool = False):
    if all_:
        if force or confirm("Delete ALL repositories and packages?"):
            for r in db.get_repositories():
                db.delete_repository(r["id"])
            _logger.info("Deleted all repositories.")
        return
    if not name:
        print("Error: repository name required (or use --all)")
        sys.exit(1)
    repo = db.get_repo_by_name(name)
    if not repo:
        print(f"Repository '{name}' not found.")
        sys.exit(1)
    if force or confirm(f"Delete repository '{name}'?"):
        db.delete_repository(repo["id"])
        _logger.info(f"Deleted repository '{name}'")


def confirm(prompt: str) -> bool:
    try:
        return input(f"{prompt} [y/N]: ").strip().lower() == "y"
    except EOFError:
        return False


# -------------------------------
# Search packages
# -------------------------------
def search(patterns: list, repoids: Optional[list] = None, showduplicates: bool = False):
    if not patterns:
        print("No search patterns provided.")
        return
    matched_pkgs = db.search_packages(patterns, repo_names=repoids)
    if not matched_pkgs:
        print("No packages matched your search.")
        return
    matched_pkgs = [dict(p) for p in matched_pkgs]

    if not showduplicates:
        newest = {}
        for pkg in matched_pkgs:
            key = (pkg["name"], pkg["arch"])
            if key not in newest or (pkg.get("epoch", 0), pkg.get("version", ""), pkg.get("release", "")) > (
                newest[key].get("epoch", 0),
                newest[key].get("version", ""),
                newest[key].get("release", ""),
            ):
                newest[key] = pkg
        matched_pkgs = list(newest.values())

    matched_pkgs.sort(
        key=lambda p: (p.get("name", "").lower(), -p.get("epoch", 0), p.get("version", ""), p.get("release", ""))
    )
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
    repo_names = repoids if repoids else None

    initial = db.search_packages(packages, repo_names=repo_names)
    if not initial:
        print("No matching packages.")
        return

    resolved_pkgs = db.resolve_dependencies(initial, include_weak=weakdeps, recursive=recursive)

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
    packages: List[str],
    repoids: Optional[List[str]] = None,
    downloaddir: Optional[str] = None,
    resolve_deps: bool = False,
    source: bool = False,
    urls: bool = False,
    arch: Optional[str] = None,
):
    downloaddir = downloaddir or "."
    _logger.info(f"Download directory: {downloaddir}")

    all_pkgs = db.search_packages(patterns=["*"], repo_names=repoids)
    if not all_pkgs:
        print("No packages found in repository selection.")
        return

    matched: List[sqlite3.Row] = [p for p in all_pkgs if match_patterns(p["name"], packages)]
    if not matched:
        print("No packages matched download request.")
        return

    matched = [dict(p) for p in matched]

    if resolve_deps:
        deps = db.resolve_dependencies(matched, include_weak=False, recursive=True)
        known_ids = {p["id"] for p in matched}
        for d in deps:
            if d["id"] not in known_ids:
                matched.append(dict(d))

    if source:
        try:
            matched = db.to_source_packages(matched)
        except AttributeError:
            print("Source package conversion not implemented in this build.")
            return

    if arch:
        matched = [p for p in matched if p.get("arch") == arch]

    if not matched:
        print("Nothing to download after filtering.")
        return

    if urls:
        for p in matched:
            if p.get("url"):
                print(p["url"])
        return

    for p in matched:
        _logger.info(f"Downloading {p['name']}-{p['version']}-{p['release']} ({p.get('arch')}) …")
        try:
            downloader.download(p, dest=downloaddir)
        except Exception as e:
            _logger.error(f"Failed to download {p['name']}: {e}")

    _logger.info(f"Downloaded {len(matched)} packages.")
