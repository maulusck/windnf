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

# -------------------------------------------------------
# Initialize shared services
# -------------------------------------------------------
config = Config()
db = DbManager(config.db_path)
downloader = Downloader(config)
metadata_mgr = MetadataManager(downloader, db)


# -------------------------------------------------------
# Helper: pattern matching (DNF-style)
# -------------------------------------------------------
def match_patterns(name: str, patterns: List[str]) -> bool:
    for pat in patterns:
        if "*" not in pat and "?" not in pat:
            # substring match (like dnf search)
            if pat in name:
                return True
        else:
            # wildcard match
            if fnmatch.fnmatch(name, pat):
                return True
    return False


# -------------------------------------------------------
# Repository commands
# -------------------------------------------------------
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


def reposync(names: Optional[List[str]], all_: bool):
    if all_:
        repos = db.get_repositories()
    else:
        repos = []
        for n in names:
            r = db.get_repo_by_name(n)
            if not r:
                _logger.warning(f"Repository '{n}' not found, skipping.")
            else:
                repos.append(r)

    for repo in repos:
        _logger.info(f"Syncing {repo['name']} …")
        metadata_mgr.sync_repo(repo)


def repodel(name: Optional[str], force: bool, all_: bool):
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


# -------------------------------------------------------------
# Search packages
# -------------------------------------------------------------
def search(patterns: list, repoids: Optional[list] = None, search_all: bool = False, showduplicates: bool = False):
    """
    Search for packages matching given patterns.

    Args:
        patterns (list[str]): Patterns to search (supports substrings and wildcards).
        repoids (list[str], optional): Restrict search to these repository names.
        search_all (bool): If True, also search in description/URL (not implemented yet).
        showduplicates (bool): If True, show all versions; otherwise only newest per package/arch.
    """
    if not patterns:
        print("No search patterns provided.")
        return

    # Fetch matching packages from DB
    matched_pkgs = db.search_packages(patterns, repo_names=repoids)

    if not matched_pkgs:
        print("No packages matched your search.")
        return

    # Filter to newest per (name, arch) if duplicates not requested
    if not showduplicates:
        newest = {}
        for pkg in matched_pkgs:
            key = (pkg["name"], pkg["arch"])
            if key not in newest:
                newest[key] = pkg
            else:
                # Compare epoch, version, release
                current = pkg
                existing = newest[key]
                if (current["epoch"], current["version"], current["release"]) > (
                    existing["epoch"],
                    existing["version"],
                    existing["release"],
                ):
                    newest[key] = current
        matched_pkgs = list(newest.values())

    # Sort results by name, version, release
    matched_pkgs.sort(key=lambda p: (p["name"].lower(), p["epoch"], p["version"], p["release"]), reverse=False)

    # Display
    for pkg in matched_pkgs:
        print(f"{pkg['name']:<30} {pkg['version']}-{pkg['release']:<15} {pkg['arch']:<7} {pkg['repo_name']}")


# -------------------------------------------------------
# Resolve dependencies
# -------------------------------------------------------
def resolve(
    packages: List[str],
    repoids: Optional[List[str]] = None,
    recursive: bool = False,
    weakdeps: bool = False,
    arch: Optional[str] = None,
):
    for pkg in packages:
        info = db.lookup_exact(pkg, repoids=repoids, arch=arch)
        if not info:
            print(f"Package '{pkg}' not found.")
            continue

        print(f"{info['name']} {info['version']}-{info['release']} ({info['arch']})")

        deps = db.get_dependencies(info, weakdeps=weakdeps)
        for d in deps:
            print(f" ├─ {d}")

        if recursive:
            print(" (Recursive resolution not implemented)")


# -------------------------------------------------------
# Download packages
# -------------------------------------------------------
def download(
    packages: List[str],
    repoids: Optional[List[str]] = None,
    downloaddir: Optional[str] = None,
    resolve: bool = False,
    source: bool = False,
    urls: bool = False,
    arch: Optional[str] = None,
):
    if not downloaddir:
        downloaddir = "."

    _logger.info(f"Download directory: {downloaddir}")

    # Match packages
    all_pkgs = db.search_packages(repoids=repoids, patterns=["*"])
    matched = [p for p in all_pkgs if match_patterns(p["name"], packages)]

    if not matched:
        print("No packages matched download request.")
        return

    # Resolve dependencies
    if resolve:
        deps = db.resolve_dependencies(matched, arch=arch)
        matched.extend([d for d in deps if d not in matched])

    # Convert to source packages if requested
    if source:
        matched = db.to_source_packages(matched)

    # Print URLs only
    if urls:
        for p in matched:
            print(p["url"])
        return

    # Perform downloads
    for p in matched:
        _logger.info(f"Downloading {p['name']} …")
        downloader.download(p, dest=downloaddir)

    _logger.info(f"Downloaded {len(matched)} packages.")
