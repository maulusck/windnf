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


def search_packages(
    db_manager: DbManager,
    pattern: str,
    repo_names: Optional[List[str]] = None,
    showduplicates: bool = False,
) -> None:
    pkgs = db_manager.search_packages(pattern, repo_names)
    if not pkgs:
        _logger.info("No packages found matching the pattern.")
        return

    if not showduplicates:
        seen = set()
        filtered = []
        for pkg in pkgs:
            key = (pkg["name"], pkg["repo_name"])
            if key not in seen:
                seen.add(key)
                filtered.append(pkg)
        pkgs = filtered

    print(f"Search results for pattern '{pattern}':")
    for pkg in pkgs:
        print(f"{pkg['name']:<30} {pkg['repo_name']:<15} Ver: {pkg['version']}-{pkg['release']} Arch: {pkg['arch']}")


def _get_direct_dependencies(
    db_manager: DbManager,
    package_name: str,
    repo_names: Optional[List[str]],
    include_weak: bool,
) -> List[str]:
    deps = set()
    matched_pkgs = db_manager.search_packages(package_name, repo_names)
    for pkg in matched_pkgs:
        capabilities = db_manager.get_dependencies_for_package(pkg["id"], include_weak)
        for cap in capabilities:
            providers = db_manager.get_packages_providing(cap, repo_names)
            provider_names = {p["name"] for p in providers}
            deps.update(provider_names)
    return list(deps)


def _gather_dependency_tree(
    db_manager: DbManager,
    package_names: List[str],
    repo_names: Optional[List[str]],
    include_weak: bool,
    max_depth: int = 50,
) -> Set[str]:
    resolved: Set[str] = set()
    visiting: Set[str] = set()

    def recurse(pkgs: List[str], depth: int):
        if depth > max_depth:
            _logger.warning(f"Max dependency depth {max_depth} reached, stopping recursion")
            return
        for p in pkgs:
            if p in resolved:
                continue
            if p in visiting:
                _logger.error(f"Circular dependency detected on package '{p}'")
                continue
            visiting.add(p)
            resolved.add(p)
            direct_deps = _get_direct_dependencies(db_manager, p, repo_names, include_weak)
            recurse(direct_deps, depth + 1)
            visiting.remove(p)

    recurse(package_names, 0)
    return resolved


def print_dependencies_tabular(package_name: str, dependencies_info: List[dict]) -> None:
    print(f"Dependencies for package: {package_name}")
    print(f"{'Package Name':<40}{'Repo':<20}{'Version':<15}{'Release':<15}{'Epoch':<8}{'Arch':<10}")
    print("-" * 108)
    for info in sorted(dependencies_info, key=lambda p: p["name"]):
        print(
            f"{info['name']:<40}"
            f"{info['repo_name']:<20}"
            f"{info['version']:<15}"
            f"{info['release']:<15}"
            f"{info['epoch']:<8}"
            f"{info['arch']:<10}"
        )


def resolve_dependencies(
    db_manager: DbManager,
    package_name: str,
    repo_names: Optional[List[str]] = None,
    recurse: bool = False,
    include_weak: bool = False,
    showduplicates: bool = False,
) -> None:
    matched_pkgs = db_manager.search_packages(package_name, repo_names)
    if not matched_pkgs:
        repo_str = f" in repos {repo_names}" if repo_names else ""
        _logger.error(f"Package '{package_name}' not found{repo_str}.")
        return

    root_pkgs = list({pkg["name"] for pkg in matched_pkgs})

    for root_pkg in root_pkgs:
        if recurse:
            all_pkgs = _gather_dependency_tree(db_manager, [root_pkg], repo_names, include_weak)
        else:
            all_pkgs = {root_pkg}
            all_pkgs.update(_get_direct_dependencies(db_manager, root_pkg, repo_names, include_weak))

        if not all_pkgs:
            _logger.info(f"No dependencies resolved for package '{root_pkg}'.")
            continue

        pkgs_info = []
        for pkg_name in all_pkgs:
            pkgs = db_manager.search_packages(pkg_name, repo_names)
            if pkgs:
                pkgs_info.extend(pkgs)

        if not showduplicates:
            seen = set()
            filtered = []
            for pkg in pkgs_info:
                key = (pkg["name"], pkg["repo_name"])
                if key not in seen:
                    seen.add(key)
                    filtered.append(pkg)
            pkgs_info = filtered

        print_dependencies_tabular(root_pkg, pkgs_info)


def download_packages(
    db_manager: DbManager,
    metadata_manager: MetadataManager,
    config: Config,
    package_names: List[str],
    repo_names: Optional[List[str]] = None,
    download_deps: bool = False,
    recurse: bool = False,
    include_weak: bool = False,
    fetchduplicates: bool = False,
    max_depth: int = 50,
) -> None:
    if download_deps:
        all_packages = (
            _gather_dependency_tree(db_manager, package_names, repo_names, include_weak, max_depth)
            if recurse
            else set(package_names).union(
                set(
                    dep
                    for pkg in package_names
                    for dep in _get_direct_dependencies(db_manager, pkg, repo_names, include_weak)
                )
            )
        )
    else:
        all_packages = set(package_names)

    if not all_packages:
        _logger.info("No packages to download.")
        return

    if not fetchduplicates:
        filtered = {}
        for name in all_packages:
            rows = db_manager.search_packages(name, repo_names)
            for pkg in rows:
                key = (pkg["name"], pkg["repo_name"])
                if key not in filtered:
                    filtered[key] = pkg
        package_infos = list(filtered.values())
    else:
        package_infos = []
        for name in all_packages:
            rows = db_manager.search_packages(name, repo_names)
            if rows:
                package_infos.extend(rows)

    download_dir = config.download_path
    download_dir.mkdir(parents=True, exist_ok=True)
    _logger.info(f"Downloading packages: {', '.join(sorted(all_packages))}")

    for pkg_name in sorted(all_packages):
        pkg_info = _find_package_info(db_manager, pkg_name, repo_names)
        if not pkg_info:
            _logger.warning(f"Package {pkg_name} not found in configured repos.")
            continue

        url = urljoin(pkg_info["base_url"], pkg_info["filepath"])
        if not url:
            _logger.warning(f"Skipping {pkg_name}: no valid download URL found")
            continue

        dest_file = download_dir / Path(pkg_info["filepath"]).name
        if dest_file.exists():
            _logger.info(f"Already downloaded: {dest_file.name}")
            continue
        try:
            metadata_manager.downloader.download(url, dest_file)
            _logger.info(f"Downloaded: {dest_file.name}")
        except Exception as e:
            _logger.error(f"Failed to download {dest_file.name}: {e}")


def _find_package_info(db_manager: DbManager, package_name: str, repo_names: Optional[List[str]]):
    rows = db_manager.search_packages(package_name, repo_names)
    if not rows:
        return None
    pkg = rows[0]
    base_url = db_manager.get_base_url_for_package(pkg["id"])
    if not base_url:
        return None
    return {"base_url": base_url, "filepath": pkg["filepath"]}


def print_package_table(pkgs_info: List[dict]) -> None:
    if not pkgs_info:
        print("No packages to display.")
        return

    headers = ["Package Name", "Repo", "Version", "Release", "Epoch", "Arch"]
    col_widths = [30, 15, 10, 10, 6, 8]

    header_row = "".join(f"{h:<{w}}" for h, w in zip(headers, col_widths))
    print(header_row)
    print("-" * len(header_row))

    for pkg in sorted(pkgs_info, key=lambda p: p["name"]):
        print(
            f"{pkg['name']:<30}"
            f"{pkg['repo_name']:<15}"
            f"{pkg['version']:<10}"
            f"{pkg['release']:<10}"
            f"{pkg['epoch']:<6}"
            f"{pkg['arch']:<8}"
        )
