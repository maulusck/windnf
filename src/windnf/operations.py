import sys
from typing import List, Optional

from .config import Config
from .db_manager import DbManager
from .downloader import Downloader
from .logger import setup_logger
from .metadata_manager import MetadataManager

_logger = setup_logger()

# Initialize shared objects
config = Config()
db = DbManager(config.db_path)
downloader = Downloader(config)  # Assumes Downloader takes Config instance
metadata_mgr = MetadataManager(downloader, db)


def repoadd(name: str, baseurl: str, repomd: str) -> None:
    """Add or update a repository in the database."""
    db.add_repository(name, baseurl, repomd)
    _logger.info(f"Repository '{name}' added or updated with base URL: {baseurl} and repomd path: {repomd}")


def repolist() -> None:
    """List all repositories."""
    repos = db.get_repositories()
    if not repos:
        print("No repositories configured.")
    else:
        for repo in repos:
            print(f"{repo['name']:20} {repo['base_url']} (repomd: {repo['repomd_url']})")


def reposync(names: Optional[List[str]] = None, all_: bool = False) -> None:
    """Synchronize one or all repositories."""
    if not names and not all_:
        print("Error: Specify repository name(s) or use --all/-A to sync all.")
        sys.exit(1)

    to_sync = []
    if all_:
        to_sync = db.get_repositories()
    else:
        to_sync = []
        for name in names:
            repo = db.get_repo_by_name(name)
            if repo is None:
                _logger.warning(f"Repository '{name}' not found, skipping.")
            else:
                to_sync.append(repo)

    for repo in to_sync:
        metadata_mgr.sync_repo(repo)


def repodel(name: Optional[str], force: bool, all_: bool) -> None:
    """Delete a repository and all associated packages."""
    if all_:
        if force or confirm_action("Are you sure you want to delete ALL repositories and packages?"):
            repos = db.get_repositories()
            for repo in repos:
                db.delete_repository(repo["id"])
            _logger.info("Deleted all repositories and their packages.")
        else:
            print("Aborted: confirmation required to delete all repositories.")
            sys.exit(1)
    else:
        if not name:
            print("Error: repository name required unless --all/-A is specified")
            sys.exit(1)
        repo = db.get_repo_by_name(name)
        if not repo:
            print(f"Repository '{name}' not found.")
            sys.exit(1)
        if force or confirm_action(f"Are you sure you want to delete repository '{name}' and its packages?"):
            db.delete_repository(repo["id"])
            _logger.info(f"Deleted repository '{name}' and its packages.")
        else:
            print("Aborted: confirmation required to delete repository.")
            sys.exit(1)


def confirm_action(prompt: str) -> bool:
    try:
        return input(f"{prompt} [y/N]: ").strip().lower() == "y"
    except EOFError:
        return False


def search(patterns: List[str], repo: Optional[str], showduplicates: bool) -> None:
    repo_names = repo.split(",") if repo else None
    results = db.search_packages(patterns, repo_names)

    if not results:
        print("No packages found matching search criteria.")
        return

    if showduplicates:
        # Print all matching packages as-is
        for pkg in results:
            print(f"{pkg['name']} {pkg['version']}-{pkg['release']} ({pkg['arch']}) repo: {pkg['repo_name']}")
    else:
        # Only print the latest version per package name
        latest_per_name = {}
        for pkg in results:
            name = pkg["name"]
            key = (
                int(pkg["epoch"]),
                pkg["version"],
                pkg["release"],
            )
            # Update if new version is greater (epoch, version, release)
            # Version comparison here assumes lexical order, may customize if needed
            if name not in latest_per_name:
                latest_per_name[name] = pkg
            else:
                # Compare tuple keys lexically to pick latest version
                current_key = (
                    int(latest_per_name[name]["epoch"]),
                    latest_per_name[name]["version"],
                    latest_per_name[name]["release"],
                )
                if key > current_key:
                    latest_per_name[name] = pkg

        for pkg in latest_per_name.values():
            print(f"{pkg['name']} {pkg['version']}-{pkg['release']} ({pkg['arch']}) repo: {pkg['repo_name']}")


def resolve(packages: List[str], repo: Optional[str], recurse: bool, weakdeps: bool) -> None:
    """
    Resolve dependencies for given packages.
    This simple example just prints the packages received.
    Full recursive resolution logic would be more involved.
    """
    repo_names = repo.split(",") if repo else None

    for pkg in packages:
        # Simplified illustrative: show package info if found
        pkg_info = None
        if repo_names:
            for r in repo_names:
                pkg_info = db.get_package_by_name_repo(r, pkg)
                if pkg_info:
                    break
        else:
            # Search all repos
            results = db.search_packages([pkg])
            pkg_info = results[0] if results else None

        if not pkg_info:
            print(f"Package '{pkg}' not found")
            continue

        print(f"Package: {pkg_info['name']} Version: {pkg_info['version']} Release: {pkg_info['release']}")

        # For actual dependency tree resolution, call db.get_dependencies_for_package as needed.
        # Recursion and weakdep handling would be implemented here in a real system.

    if recurse:
        print("(Recursive dependency resolution not implemented in this example)")

    if weakdeps:
        print("(Including weak dependencies in resolution)")


def download(
    packages: List[str],
    repo: Optional[str],
    alldeps: bool,
    recurse: bool,
    weakdeps: bool,
    fetchduplicates: bool,
    url: bool,
) -> None:
    """
    Download packages and dependencies as instructed.
    For now this will just print intended actions.
    """
    repo_names = repo.split(",") if repo else None

    print(f"Downloading packages: {', '.join(packages)}")
    print(f"Repositories: {repo_names or 'all'}")
    print(f"Download all dependencies: {alldeps}")
    print(f"Recursively: {recurse}")
    print(f"Weak dependencies: {weakdeps}")
    print(f"Fetch duplicates: {fetchduplicates}")
    print(f"Print URLs only (no download): {url}")

    # Real implementation would:
    # 1. Resolve dependencies according to flags.
    # 2. Fetch and download packages or print their URLs.
    # 3. Handle duplicates and weak dependencies accordingly.

    print("(Download implementation to be added)")
