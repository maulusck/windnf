from pathlib import Path
from typing import List, Optional, Set
from urllib.parse import urljoin

from .config import Config
from .db_manager import DbManager
from .metadata_manager import MetadataManager
from .utils import _logger


def add_repo(db_manager: DbManager, name: str, base_url: str, repomd_path: str = "repodata/repomd.xml") -> None:
    repomd_url = urljoin(base_url, repomd_path)
    db_manager.add_repository(name, base_url, repomd_url)
    _logger.info(f"Added repository '{name}' with base URL {base_url}")


def list_repos(db_manager: DbManager) -> None:
    repos = db_manager.get_repositories()
    if not repos:
        _logger.info("No repositories configured.")
        return
    print("Repositories:")
    for repo in repos:
        last_updated = repo["last_updated"] or "never"
        print(f"  {repo['name']}: {repo['base_url']} (Last updated: {last_updated})")


def sync_repos(metadata_manager: MetadataManager, db_manager: DbManager, repo_name: Optional[str] = None) -> None:
    if repo_name is None or repo_name.lower() == "all":
        repos = db_manager.get_repositories()
    else:
        repo = db_manager.get_repo_by_name(repo_name)
        if repo is None:
            _logger.error(f"Repository '{repo_name}' not found.")
            return
        repos = [repo]

    for repo in repos:
        try:
            metadata_manager.sync_repo(repo)
        except Exception as e:
            _logger.error(f"Failed to sync repository '{repo['name']}': {e}")


def delete_repo(db_manager: DbManager, repo_name: str) -> None:
    repo = db_manager.get_repo_by_name(repo_name)
    if not repo:
        _logger.error(f"Repository '{repo_name}' not found.")
        return
    try:
        _logger.info(f"Deleting repository '{repo_name}' and all its packages and dependencies.")
        db_manager.delete_repository(repo["id"])
        _logger.info(f"Repository '{repo_name}' deleted successfully.")
    except Exception as e:
        _logger.error(f"Failed to delete repository '{repo_name}': {e}")


def search_packages(db_manager: DbManager, pattern: str, repo_names: Optional[List[str]] = None) -> None:
    pkgs = db_manager.search_packages(pattern, repo_names)
    if not pkgs:
        _logger.info("No packages found matching the pattern.")
        return
    print(f"Search results for pattern '{pattern}':")
    for pkg in pkgs:
        print(f"{pkg['name']:<30} {pkg['repo_name']:<15} Ver: {pkg['version']}-{pkg['release']} Arch: {pkg['arch']}")


def resolve_dependencies(
    db_manager: DbManager,
    package_name: str,
    repo_name: Optional[str] = None,
    recurse: bool = False,
    include_weak: bool = False,
) -> None:
    # Find package first
    pkg_row = None
    if repo_name:
        pkg_row = db_manager.get_package_by_name_repo(repo_name, package_name)
        if not pkg_row:
            _logger.error(f"Package '{package_name}' not found in repo '{repo_name}'.")
            return
    else:
        for repo in db_manager.get_repositories():
            candidate = db_manager.get_package_by_name_repo(repo["name"], package_name)
            if candidate:
                pkg_row = candidate
                break
        if not pkg_row:
            _logger.error(f"Package '{package_name}' not found in any repository.")
            return

    resolved_ids: Set[int] = set()
    to_resolve: Set[int] = {pkg_row["id"]}
    id_to_info = {}

    def load_info(pkg_id: int):
        if pkg_id in id_to_info:
            return id_to_info[pkg_id]
        info = db_manager.get_package_info_by_id(pkg_id)
        if info:
            id_to_info[pkg_id] = info
        return info

    while to_resolve:
        current = to_resolve.pop()
        if current in resolved_ids:
            continue
        resolved_ids.add(current)
        dep_ids = db_manager.get_required_package_ids(current, include_weak)
        if recurse:
            to_resolve.update(dep_ids - resolved_ids)
        else:
            resolved_ids.update(dep_ids)

    print(f"{'Package Name':<30}{'Repo':<15}{'Version':<10}{'Release':<10}{'Epoch':<6}{'Arch':<8}")
    print("-" * 79)
    for pid in sorted(resolved_ids, key=lambda x: id_to_info.get(x, {}).get("name", "")):
        info = load_info(pid)
        if info:
            print(
                f"{info['name']:<30}"
                f"{info['repo_name']:<15}"
                f"{info['version']:<10}"
                f"{info['release']:<10}"
                f"{info['epoch']:<6}"
                f"{info['arch']:<8}"
            )


def download_packages(
    db_manager: DbManager,
    metadata_manager: MetadataManager,
    config: Config,
    package_names: List[str],
    repo_names: Optional[List[str]] = None,
    download_deps: bool = False,
    recurse: bool = False,
    include_weak: bool = False,
    max_depth: int = 50,
) -> None:
    # If recurse is set but not download_deps, enable download_deps implicitly
    if recurse and not download_deps:
        download_deps = True

    download_dir = config.download_path
    download_dir.mkdir(parents=True, exist_ok=True)

    to_download = set()

    def gather_deps(pkg_name: str, depth: int = 0):
        to_download.add(pkg_name)
        if download_deps:
            if not recurse or (recurse and depth < max_depth):
                # Get dependencies by capability mapping to package names
                deps = _get_direct_dependencies(db_manager, pkg_name, repo_names, include_weak)
                for dep in deps:
                    if dep not in to_download:
                        if recurse:
                            gather_deps(dep, depth + 1)
                        else:
                            to_download.add(dep)

    for pkg in package_names:
        gather_deps(pkg)

    if not to_download:
        _logger.info("No packages to download.")
        return

    _logger.info(f"Downloading packages: {', '.join(sorted(to_download))}")

    for pkg in sorted(to_download):
        pkg_info = _find_package_info(db_manager, pkg, repo_names)
        if not pkg_info:
            _logger.warning(f"Package {pkg} not found in configured repos.")
            continue

        url = urljoin(pkg_info["base_url"], pkg_info["filepath"])
        dest_file = download_dir / Path(pkg_info["filepath"]).name
        if dest_file.exists():
            _logger.info(f"Already downloaded: {dest_file.name}")
            continue
        try:
            metadata_manager.downloader.download(url, dest_file)
            _logger.info(f"Downloaded: {dest_file.name}")
        except Exception as e:
            _logger.error(f"Failed to download {dest_file.name}: {e}")


def _get_direct_dependencies(
    db_manager: DbManager,
    package_name: str,
    repo_names: Optional[List[str]],
    include_weak: bool,
) -> List[str]:
    """
    Resolve direct dependencies of all packages matching package_name into concrete package names.
    """
    deps = set()
    matched_pkgs = db_manager.search_packages(package_name, repo_names)
    for pkg in matched_pkgs:
        capabilities = db_manager.get_dependencies_for_package(pkg["id"], include_weak)
        for cap in capabilities:
            providers = db_manager.get_packages_providing(cap, repo_names)
            provider_names = {p["name"] for p in providers}
            deps.update(provider_names)
    return list(deps)


def _find_package_info(db_manager: DbManager, package_name: str, repo_names: Optional[List[str]]):
    rows = db_manager.search_packages(package_name, repo_names)
    if not rows:
        return None
    pkg = rows[0]
    base_url = db_manager.get_base_url_for_package(pkg["id"])
    if not base_url:
        return None
    return {"base_url": base_url, "filepath": pkg["filepath"]}
