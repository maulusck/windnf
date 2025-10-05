from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

from packaging.version import InvalidVersion, Version

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
    if not repo_name or repo_name.lower() == "all":
        repos = db_manager.get_repositories()
    else:
        repo = db_manager.get_repo_by_name(repo_name)
        if not repo:
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


def _filter_latest_versions(packages: List[Dict]) -> List[Dict]:
    latest_pkgs = {}

    def version_key(pkg):
        try:
            return Version(pkg["version"] + "-" + pkg["release"])
        except InvalidVersion:
            return pkg["version"] + "-" + pkg["release"]

    for pkg in packages:
        key = (pkg["name"], pkg["repo_name"])
        current = latest_pkgs.get(key)
        if not current or version_key(pkg) > version_key(current):
            latest_pkgs[key] = pkg

    return list(latest_pkgs.values())


def search_packages(
    db_manager: DbManager,
    patterns: List[str],
    repo_names: Optional[List[str]] = None,
    showduplicates: bool = False,
    exact_match: bool = False,
) -> List[Dict]:
    all_pkgs = []
    for pattern in patterns:
        rows = db_manager.search_packages(pattern, repo_names, exact_match=exact_match, full_info=True)
        all_pkgs.extend(rows)

    if not all_pkgs:
        _logger.info("No packages found matching the given patterns.")
        return []

    pkgs = [dict(pkg) for pkg in all_pkgs]
    if not showduplicates:
        pkgs = _filter_latest_versions(pkgs)
    return pkgs


from typing import Dict, List, Optional, Set


def _get_direct_dependencies(
    db_manager: DbManager,
    package_name: str,
    repo_names: Optional[List[str]],
    include_weak: bool,
) -> Set[str]:
    deps: Set[str] = set()
    matched_pkgs = db_manager.search_packages(package_name, repo_names, exact_match=True, full_info=True)

    for pkg in matched_pkgs:
        pkg_dict = dict(pkg)
        pkg_id = pkg_dict.get("id")
        if pkg_id is None:
            _logger.warning(f"Package missing 'id' field, skipping: {pkg}")
            continue

        capabilities = set(db_manager.get_dependencies_for_package(pkg_id, include_weak))

        for cap in capabilities:
            providers = db_manager.get_packages_providing(cap, repo_names)
            provider_names = {p["name"] for p in providers}
            deps.update(provider_names)

    return deps


def resolve_dependencies_single(
    db_manager: DbManager,
    package_name: str,
    repo_names: Optional[List[str]] = None,
    recurse: bool = False,
    include_weak: bool = False,
    max_depth: int = 50,
) -> Set[str]:
    resolved: Set[str] = set()
    visiting: Set[str] = set()

    def _recurse(pkgs: Set[str], depth: int) -> None:
        if depth > max_depth:
            _logger.warning(f"Max dependency depth {max_depth} reached, stopping recursion")
            return
        for p in pkgs:
            p_str = p if isinstance(p, str) else str(p)

            if p_str in resolved:
                continue
            if p_str in visiting:
                _logger.error(f"Circular dependency detected on package '{p_str}'")
                continue
            visiting.add(p_str)
            resolved.add(p_str)

            direct_deps_objs = _get_direct_dependencies(db_manager, p, repo_names, include_weak)
            direct_deps = {dep if isinstance(dep, str) else str(dep) for dep in direct_deps_objs}

            if recurse:
                _recurse(direct_deps, depth + 1)
            else:
                resolved.update(direct_deps)
            visiting.remove(p_str)

    initial_pkg = package_name if isinstance(package_name, str) else str(package_name)
    _recurse({initial_pkg}, 0)
    return resolved


def resolve_dependencies_multiple(
    db_manager: DbManager,
    package_names: List[str],
    repo_names: Optional[List[str]] = None,
    recurse: bool = False,
    include_weak: bool = False,
) -> Dict[str, Set[str]]:
    results: Dict[str, Set[str]] = {}
    for pkg in package_names:
        results[pkg] = resolve_dependencies_single(db_manager, pkg, repo_names, recurse, include_weak)
    return results


def _filter_latest_versions(packages: list) -> list:
    latest_pkgs = {}

    def version_key(pkg):
        try:
            return Version(pkg["version"] + "-" + pkg["release"])
        except InvalidVersion:
            return pkg["version"] + "-" + pkg["release"]

    for pkg in packages:
        key = (pkg["name"], pkg["repo_name"])
        current = latest_pkgs.get(key)
        if not current or version_key(pkg) > version_key(current):
            latest_pkgs[key] = pkg

    return list(latest_pkgs.values())


def download_packages(
    db_manager: DbManager,
    metadata_manager: MetadataManager,
    config: Config,
    package_names: list,
    repo_names: list = None,
    download_deps: bool = False,
    recurse: bool = False,
    include_weak: bool = False,
    fetchduplicates: bool = False,
    max_depth: int = 50,
) -> None:
    if download_deps:
        all_packages = set()
        for pkg in package_names:
            all_packages.update(
                resolve_dependencies_single(db_manager, pkg, repo_names, recurse, include_weak, max_depth)
            )
        all_packages.update(package_names)
    else:
        all_packages = set(package_names)

    if not all_packages:
        _logger.info("No packages to download.")
        return

    package_infos = []
    for pkg_name in all_packages:
        pkgs = db_manager.search_packages(pkg_name, repo_names, exact_match=True, full_info=True)
        if not pkgs:
            _logger.warning(f"Package '{pkg_name}' not found or missing; skipping.")
            continue
        package_infos.extend([dict(pkg) for pkg in pkgs])

    if not package_infos:
        _logger.info("No valid package info found for download.")
        return

    if not fetchduplicates:
        package_infos = _filter_latest_versions(package_infos)

    seen_keys = set()
    filtered_packages = []
    for p in package_infos:
        base_url = db_manager.get_base_url_for_package(p["id"])
        if not base_url:
            _logger.warning(f"Package '{p['name']}' missing base URL; skipping.")
            continue
        p["base_url"] = base_url
        key = (p["repo_name"], p["name"], p["version"], p["release"], p["epoch"], p["arch"])
        if fetchduplicates or key not in seen_keys:
            seen_keys.add(key)
            filtered_packages.append(p)

    download_dir = config.download_path
    download_dir.mkdir(parents=True, exist_ok=True)
    _logger.info(f"Downloading packages: {', '.join(sorted({p['name'] for p in filtered_packages}))}")

    for pkg_info in filtered_packages:
        url = urljoin(pkg_info["base_url"], pkg_info["filepath"])
        if not url:
            _logger.warning(f"Skipping {pkg_info['name']}: no valid download URL found")
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
