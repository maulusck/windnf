# operations.py
import fnmatch
import sys
from typing import Dict, List, Optional

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
            if pat in name:  # substring match
                return True
        else:
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


def reposync(names: Optional[List[str]] = None, all_: bool = False):
    if all_:
        repos = db.get_repositories()
    else:
        repos = []
        for n in names or []:
            r = db.get_repo_by_name(n)
            if not r:
                _logger.warning(f"Repository '{n}' not found, skipping.")
            else:
                repos.append(r)

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


# -------------------------------------------------------
# Search packages
# -------------------------------------------------------
def search(patterns: list, repoids: Optional[list] = None, search_all: bool = False, showduplicates: bool = False):
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
            if key not in newest:
                newest[key] = pkg
            else:
                current = pkg
                existing = newest[key]
                if (current.get("epoch", 0), current.get("version", ""), current.get("release", "")) > (
                    existing.get("epoch", 0),
                    existing.get("version", ""),
                    existing.get("release", ""),
                ):
                    newest[key] = current
        matched_pkgs = list(newest.values())

    # Sort by name, epoch, version, release descending for newest first
    matched_pkgs.sort(
        key=lambda p: (p.get("name", "").lower(), -p.get("epoch", 0), p.get("version", ""), p.get("release", "")),
        reverse=False,
    )

    # Print results
    for pkg in matched_pkgs:
        name = pkg.get("name", "")
        version = pkg.get("version", "")
        release = pkg.get("release", "")
        arch = pkg.get("arch", "")
        repo_name = pkg.get("repo_name", "")
        summary = pkg.get("summary") or ""

        # Format main line with fixed width columns for clean alignment
        print(f"{name:<40} {version}-{release:<20} {arch:<10} {repo_name}")
        if summary:
            print(f"    {summary}")


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
    to_process = packages[:]
    seen = set()

    while to_process:
        pkg_name = to_process.pop(0)
        if pkg_name in seen:
            continue
        seen.add(pkg_name)

        pkg_info = db.lookup_exact(pkg_name, repoids=repoids, arch=arch)
        if not pkg_info:
            print(f"Package '{pkg_name}' not found.")
            continue

        print(f"{pkg_info['name']}-{pkg_info['version']}-{pkg_info['release']} ({pkg_info['arch']})")

        deps = db.get_dependencies(pkg_info, weakdeps=weakdeps)
        for d in deps:
            print(f" ├─ {d}")
            if recursive and d not in seen:
                to_process.append(d)


# -------------------------------------------------------
# Download packages
# -------------------------------------------------------
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

    # Fetch all packages in repo(s)
    all_pkgs = db.search_packages(repo_names=repoids, patterns=["*"])
    matched = [p for p in all_pkgs if match_patterns(p["name"], packages)]
    if not matched:
        print("No packages matched download request.")
        return

    # Resolve dependencies if requested
    if resolve_deps:
        deps = db.resolve_dependencies(matched, arch=arch)
        matched += [d for d in deps if d not in matched]

    # Convert to source packages
    if source:
        matched = db.to_source_packages(matched)

    # Print URLs only
    if urls:
        for p in matched:
            print(p["url"])
        return

    # Download packages
    for p in matched:
        _logger.info(f"Downloading {p['name']}-{p['version']}-{p['release']} …")
        downloader.download(p, dest=downloaddir)

    _logger.info(f"Downloaded {len(matched)} packages.")
