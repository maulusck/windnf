# operations.py
from __future__ import annotations

import fnmatch
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

from .config import Config
from .db_manager import DbManager
from .downloader import Downloader
from .logger import Colors, setup_logger
from .metadata_manager import MetadataManager
from .nevra import NEVRA

_logger = setup_logger()

_cfg: Optional[Config] = None
db: Optional[DbManager] = None
metadata: Optional[MetadataManager] = None
downloader: Optional[Downloader] = None


def init(config: Config) -> None:
    global _cfg, db, metadata, downloader
    _cfg = config
    db = DbManager(_cfg)
    downloader = Downloader(_cfg)
    metadata = MetadataManager(_cfg, db, downloader, max_workers=4)
    _logger.debug("operations initialized with DB=%s, downloader=%s", _cfg.db_path, _cfg.downloader)


def _ensure_initialized() -> None:
    if not all((_cfg, db, metadata, downloader)):
        raise RuntimeError("operations not initialized; call operations.init(config) first")


def _resolve_repo_names_to_ids(repo_names: Optional[Sequence[str]]) -> Optional[List[int]]:
    if not repo_names:
        return None
    out: List[int] = []
    for name in repo_names:
        repo = db.get_repo(name)
        if not repo:
            raise ValueError(f"Repository not found: {name}")
        out.append(int(repo["id"]))
    return out


def highlight_match(text: str, pattern: str) -> str:
    if not pattern:
        return text
    pattern_re = re.compile(re.escape(pattern), re.IGNORECASE)
    return pattern_re.sub(lambda m: f"{Colors.FG_BRIGHT_RED}{Colors.BOLD}{m.group(0)}{Colors.RESET}", text)


def highlight_name_in_nevra(nevra_str: str, name: str, pattern: Optional[str]) -> str:
    if not pattern or not name:
        return nevra_str
    highlighted_name = highlight_match(name, pattern)
    escaped_name = re.escape(name)
    return re.sub(escaped_name, highlighted_name, nevra_str, count=1, flags=re.IGNORECASE)


def print_delimiter(title: str) -> None:
    width = shutil.get_terminal_size((80, 20)).columns
    delimiter = "="
    title_len = len(title) + 2
    side_len = (width - title_len) // 2
    line = delimiter * side_len + f" {title} " + delimiter * (width - side_len - title_len)
    print(line)


def repoadd(
    name: str,
    baseurl: str,
    repomd: str,
    repo_type: str,
    source_repo: Optional[str],
    sync: bool,
) -> None:
    src_id = None
    if source_repo:
        src = db.get_repo(source_repo)
        if not src:
            raise ValueError(f"Source repo not found: {source_repo}")
        src_id = int(src["id"])
    rid = db.add_repo(
        name=name,
        base_url=baseurl,
        repomd_url=repomd,
        rtype=repo_type,
        source_repo_id=src_id,
    )
    print(f"Repository '{name}' added/updated (id={rid}).")
    if not sync:
        return
    print("Starting sync...")
    repo_row = db.get_repo(int(rid))
    if repo_row:
        metadata.sync_repo(repo_row)
    else:
        print("Repo created but could not load repository row.")


def repolink(binary_repo: str, source_repo: str) -> None:
    db.link_source(binary_repo, source_repo)
    print(f"Linked binary repo '{binary_repo}' -> source repo '{source_repo}'")


def repolist() -> None:
    rows = db.list_repos()
    if not rows:
        print("No repositories configured.")
        return
    for r in rows:
        src = r.get("source_repo_id") or "-"
        print(f"{r['id']:>3} {r['name']:30} {r['base_url']:40} type={r['type']} src_id={src}")


def reposync(names: List[str], all_: bool) -> None:
    if all_:
        repos = db.list_repos()
    else:
        if not names:
            print("No repository names provided. Use --all to sync all repositories.")
            return
        repos = []
        for n in names:
            r = db.get_repo(n)
            if not r:
                print(f"Repository not found: {n}")
                continue
            repos.append(r)

    if not repos:
        print("No repositories to sync.")
        return

    for r in repos:
        print(f"Syncing {r['name']}...")
        try:
            metadata.sync_repo(r)
        except Exception as e:
            _logger.exception("Failed to sync %s: %s", r["name"], e)
            print(f"Failed to sync {r['name']}: {e}")
        else:
            print(f"Synced {r['name']}")


def repodel(names: List[str] = None, all_: bool = False, force: bool = False) -> None:
    names = names or []

    if all_:
        for repo in db.list_repos():
            if force or input(f"Delete {repo['name']}? [y/N]: ").lower() == "y":
                db.delete_repo(repo["id"])
                print(f"Deleted {repo['name']}")
        return

    if not names:
        print("No repository names or IDs provided.")
        return

    for identifier in names:
        repo = db.get_repo(identifier)
        print(repo)
        if not repo:
            print(f"Repository {identifier} not found.")
            continue

        if force or input(f"Delete repository {repo['name']}? [y/N]: ").lower() == "y":
            db.delete_repo(repo["id"])
            print(f"Deleted repository {repo['name']}")


def search(patterns: List[str], repo: List[str] = None, showduplicates: bool = False) -> None:
    repoids = _resolve_repo_names_to_ids(repo) if repo else None
    all_results: List[Dict[str, Any]] = []

    for pat in patterns:

        all_results.extend(db.search_packages(pat, repo_filter=repoids, exact=False))

    if not all_results:
        print("No packages found.")
        return

    if not showduplicates:
        latest_per_name: Dict[str, Dict[str, Any]] = {}
        for r in all_results:
            n = r["name"]
            cur = latest_per_name.get(n)
            if not cur or NEVRA.from_row(r) > NEVRA.from_row(cur):
                latest_per_name[n] = r
        results = list(latest_per_name.values())
    else:
        results = all_results

    for r in results:
        r["_name_lc"] = r.get("name", "").lower()
        r["_summary_lc"] = r.get("summary", "").lower()
        r["_nevra"] = NEVRA.from_row(r)

    for pat in patterns:
        name_summary, summary_only, name_only = [], [], []

        pat_lc = pat.lower()
        is_wildcard = "*" in pat

        for r in results:
            name, summary = r.get("name", ""), r.get("summary", "")
            name_lc, summary_lc = r["_name_lc"], r["_summary_lc"]

            if is_wildcard:
                match_name = fnmatch.fnmatchcase(name_lc, pat_lc)
                match_summary = fnmatch.fnmatchcase(summary_lc, pat_lc)
            else:
                match_name = pat_lc in name_lc
                match_summary = pat_lc in summary_lc

            if not (match_name or match_summary):
                continue

            nevra_str = str(r["_nevra"])
            disp_summary = highlight_match(summary, pat) if match_summary and not is_wildcard else summary
            nevra_disp = highlight_name_in_nevra(nevra_str, name, pat) if match_name and not is_wildcard else nevra_str

            line = f"{nevra_disp} : {disp_summary}"

            if match_name and match_summary:
                name_summary.append(line)
            elif match_summary:
                summary_only.append(line)
            elif match_name:
                name_only.append(line)

        if name_summary:
            print_delimiter(f"Name & Summary Matched: {pat}")
            for line in name_summary:
                print(line)
        if summary_only:
            print_delimiter(f"Summary Matched: {pat}")
            for line in summary_only:
                print(line)
        if name_only:
            print_delimiter(f"Name Matched: {pat}")
            for line in name_only:
                print(line)


def info(pattern: str, repo: Optional[List[str]] = None) -> None:
    repo_ids = _resolve_repo_names_to_ids(repo) if repo else None

    rows = db.search_packages(pattern, repo_filter=repo_ids, exact=True)
    if not rows:
        print("No packages match.")
        return

    best_row = max(rows, key=lambda row: NEVRA.from_row(row))
    nevra = NEVRA.from_row(best_row)

    print(f"Package: {nevra}")
    repo_name = db.get_repo(best_row["repo_id"])["name"] if best_row.get("repo_id") else best_row.get("repo_id")
    print(f" Repo: {repo_name}")
    print(f" Arch: {best_row.get('arch')}")
    print(f" Summary: {best_row.get('summary')}")
    print(f" URL: {best_row.get('url') or ''}")


def _resolve_dependencies(
    packages: List[str],
    repo: Optional[List[str]] = None,
    weakdeps: bool = False,
    recursive: bool = False,
    arch: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Resolve packages and their dependencies (without printing).

    Returns dict with:
        - 'resolved_rows': List[Dict] of resolved package rows (all included)
        - 'dep_map': Dict[int, List[Dict]] mapping pkgKey -> list of provider rows for its requirements
        - 'unsatisfied': Set[str] of unsatisfied dependency names
    """
    repo_ids = _resolve_repo_names_to_ids(repo) if repo else None

    to_resolve: List[Dict[str, Any]] = []
    for pat in packages:
        rows = db.search_packages(pat, repo_filter=repo_ids, exact=True)
        if not rows:
            continue
        best_row = max(rows, key=lambda r: NEVRA.from_row(r))
        to_resolve.append(best_row)

    if not to_resolve:
        return {"resolved_rows": [], "dep_map": {}, "unsatisfied": set()}

    provides_map = db.provides_map(repo_filter=repo_ids)
    requires_map = db.requires_map()

    resolved_keys: Set[int] = set()
    stack: List[Dict[str, Any]] = list(to_resolve)
    dep_map: Dict[int, List[Dict[str, Any]]] = {}
    unsatisfied_dependencies: Set[str] = set()

    while stack:
        pkg_row = stack.pop()
        pkgKey = pkg_row["pkgKey"]
        if pkgKey in resolved_keys:
            continue
        resolved_keys.add(pkgKey)

        reqs = requires_map.get(pkgKey, [])
        dep_map[pkgKey] = []

        for r in reqs:
            req_name = r["name"]
            provider_keys = provides_map.get(req_name, set())

            if provider_keys:
                for pKey in provider_keys:
                    prov_row = db.get_by_key(pKey, repo_filter=repo_ids)
                    if not prov_row:
                        continue
                    dep_map[pkgKey].append(prov_row)
                    if recursive and pKey not in resolved_keys:
                        stack.append(prov_row)
            else:
                unsatisfied_dependencies.add(req_name)

    resolved_rows = [db.get_by_key(k, repo_filter=repo_ids) for k in resolved_keys]
    resolved_rows = [r for r in resolved_rows if r is not None]

    return {"resolved_rows": resolved_rows, "dep_map": dep_map, "unsatisfied": unsatisfied_dependencies}


def resolve(
    packages: List[str],
    repo: Optional[List[str]] = None,
    weakdeps: bool = False,
    recursive: bool = False,
    arch: Optional[str] = None,
    verbose: bool = False,
) -> None:
    """
    CLI-facing resolver (prints packages and dependencies).
    """
    result = _resolve_dependencies(packages, repo, weakdeps, recursive, arch)
    resolved = result["resolved_rows"]
    dep_map = result["dep_map"]
    unsatisfied = result["unsatisfied"]

    if not resolved:
        print("No packages to resolve.")
        return

    printed_keys: Set[int] = set()

    for pkg_row in resolved:
        pkgKey = pkg_row["pkgKey"]
        pkg_nevra = NEVRA.from_row(pkg_row)

        if verbose:
            print_delimiter(f"Package: {pkg_nevra}")
            info(pkg_row["name"], repo)
            print("Requires:")

        deps = dep_map.get(pkgKey, [])

        if verbose:
            if deps:
                for dep_row in deps:
                    req_name = dep_row["name"]
                    prov_nevra = NEVRA.from_row(dep_row)
                    print(f"  - {req_name} provided by {prov_nevra}")
            else:
                print("  - <no dependencies>")
            print("")

        else:
            for dep_row in deps:
                depKey = dep_row["pkgKey"]
                if depKey not in printed_keys:
                    print(f"- {NEVRA.from_row(dep_row)}")
                    printed_keys.add(depKey)

    if not verbose and unsatisfied:
        print(f"\nWARNING: Unsatisfied dependencies: {', '.join(sorted(unsatisfied))}")


def download(
    packages: List[str],
    repo: Optional[List[str]],
    downloaddir: Optional[str],
    destdir: Optional[str],
    resolve_flag: bool,
    recurse: bool,
    source: bool,
    urls: bool,
    arch: Optional[str],
) -> None:
    """
    Download packages (or print URLs), including resolved dependencies if requested.

    Parameters:
    - packages: package names or NEVRA patterns
    - repo: repository filter
    - downloaddir: temp download directory
    - destdir: final destination directory (optional copy)
    - resolve_flag: resolve dependencies
    - recurse: same as resolve_flag for recursive deps
    - source: include SRPMs
    - urls: print URLs instead of downloading
    - arch: architecture filter
    """

    if resolve_flag or recurse:
        result = _resolve_dependencies(
            packages,
            repo=repo,
            weakdeps=False,
            recursive=recurse,
            arch=arch,
        )
        resolved_rows = result["resolved_rows"]
        dep_map = result["dep_map"]

        if not resolved_rows:
            print("No packages matched the patterns or dependencies.")
            return

        targets: Dict[int, Dict[str, Any]] = {r["pkgKey"]: r for r in resolved_rows}
        for deps in dep_map.values():
            for dep_row in deps:
                targets[dep_row["pkgKey"]] = dep_row
        targets_list = list(targets.values())
    else:

        targets_list: List[Dict[str, Any]] = []
        repo_ids = _resolve_repo_names_to_ids(repo)
        for p in packages:
            try:
                nv = NEVRA.parse(p)
            except Exception:
                nv = None
            rows = db.search_packages(str(nv) if nv else p, repo_filter=repo_ids, exact=True)
            if not rows:
                print(f"No match for {p}")
                continue
            best = max(rows, key=lambda r: NEVRA.from_row(r))
            targets_list.append(best)

    if not targets_list:
        print("No packages selected for download.")
        return

    download_dir = Path(downloaddir) if downloaddir else _cfg.download_path
    download_dir.mkdir(parents=True, exist_ok=True)
    dest_dir = Path(destdir) if destdir else None
    if dest_dir:
        dest_dir.mkdir(parents=True, exist_ok=True)

    def build_urls_for_row(row: Dict[str, Any]) -> List[str]:
        urls_list: List[str] = []
        lb = row.get("location_base") or row.get("locationbase") or row.get("location_base_url")
        lh = row.get("location_href") or row.get("locationhref") or row.get("href")
        if lb and lh:
            urls_list.append(f"{lb.rstrip('/')}/{lh.lstrip('/')}")
        repo_row = db.get_repo(int(row["repo_id"]))
        if repo_row and lh:
            urls_list.append(f"{repo_row['base_url'].rstrip('/')}/{lh.lstrip('/')}")
        return urls_list

    if urls:
        for row in targets_list:
            nevra = NEVRA.from_row(row)
            ulist = build_urls_for_row(row)
            if not ulist:
                print(f"{nevra} -> no URL available")
            else:
                for u in ulist:
                    print(u)
        return

    for row in targets_list:
        nevra = NEVRA.from_row(row)

        candidates = [row]
        if source and row.get("rpm_sourcerpm"):
            src_rows = db.search_packages(row["rpm_sourcerpm"], repo_filter=None, exact=True)
            candidates.extend(src_rows)

        for pkg_row in candidates:
            urls_list = build_urls_for_row(pkg_row)
            if not urls_list:
                print(f"Skipping {NEVRA.from_row(pkg_row)}: no URL available")
                continue

            url = urls_list[0]
            filename = url.split("/")[-1] or f"{NEVRA.from_row(pkg_row).to_nvra()}.rpm"
            outpath = download_dir / filename

            try:
                if hasattr(downloader, "download_to_file"):
                    downloader.download_to_file(url, outpath)
                else:
                    data = downloader.download_to_memory(url)
                    with open(outpath, "wb") as fh:
                        fh.write(data)
                print(f"Downloaded {NEVRA.from_row(pkg_row)} -> {outpath}")

                if dest_dir:
                    final = dest_dir / filename
                    try:
                        import shutil

                        shutil.copy2(outpath, final)
                        print(f"Copied to {final}")
                    except Exception as e:
                        print(f"Failed to copy to {final}: {e}")
            except Exception as e:
                _logger.exception("Download failed for %s: %s", NEVRA.from_row(pkg_row), e)
                print(f"Failed to download {NEVRA.from_row(pkg_row)}: {e}")
