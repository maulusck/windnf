from __future__ import annotations

import re
import shutil
import sys
import concurrent.futures
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from .config import Config
from .db_manager import DbManager
from .downloader import Downloader
from .logger import Colors, setup_logger
from .metadata_manager import MetadataManager
from .nevra import NEVRA, rpmvercmp 

_logger = setup_logger()

# module-level singletons (initialized by init(cfg))
_cfg: Optional[Config] = None
db: Optional[DbManager] = None
metadata: Optional[MetadataManager] = None
downloader: Optional[Downloader] = None


# -------------------------
# Initialization
# -------------------------
def init(config: Config) -> None:
    """Initialize singleton instances from config."""
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
    """Resolve repository names to IDs; None means no repo filter."""
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
    """
    Highlight occurrences of pattern using ANSI colors.
    SAFETY: Returns plain text if output is not a TTY (piped to file).
    """
    if not pattern or not sys.stdout.isatty():
        return text
        
    pattern_re = re.compile(re.escape(pattern), re.IGNORECASE)
    return pattern_re.sub(lambda m: f"{Colors.FG_BRIGHT_RED}{Colors.BOLD}{m.group(0)}{Colors.RESET}", text)


def highlight_name_in_nevra(nevra_str: str, name: str, pattern: Optional[str]) -> str:
    """Highlight only the first occurrence of name in the NEVRA string."""
    if not pattern or not name:
        return nevra_str
    highlighted_name = highlight_match(name, pattern)
    escaped_name = re.escape(name)
    return re.sub(escaped_name, highlighted_name, nevra_str, count=1, flags=re.IGNORECASE)


def print_delimiter(title: str) -> None:
    """Print a terminal-width delimiter with centered title."""
    try:
        width = shutil.get_terminal_size((80, 20)).columns
    except Exception:
        width = 80
    delimiter = "="
    title_len = len(title) + 2
    side_len = max(0, (width - title_len) // 2)
    line = delimiter * side_len + f" {title} " + delimiter * (width - side_len - title_len)
    print(line)


# -------------------------
# Repository commands
# -------------------------
def repoadd(name: str, baseurl: str, repomd: str, repo_type: str, source_repo: Optional[str]) -> None:
    """
    Add (or update) repository and sync.
    """
    _ensure_initialized()
    src_id = None
    if source_repo:
        src = db.get_repo(source_repo)
        if not src:
            raise ValueError(f"Source repo not found: {source_repo}")
        src_id = int(src["id"])

    rid = db.add_repo(name=name, base_url=baseurl, repomd_url=repomd, rtype=repo_type, source_repo_id=src_id)
    print(f"Repository '{name}' added/updated (id={rid}). Starting sync...")
    repo_row = db.get_repo(int(rid))
    if repo_row:
        metadata.sync_repo(repo_row)
        print("Sync complete.")
    else:
        print("Repo created but could not load repository row.")


def repolink(binary_repo: str, source_repo: str) -> None:
    _ensure_initialized()
    db.link_source(binary_repo, source_repo)
    print(f"Linked binary repo '{binary_repo}' -> source repo '{source_repo}'")


def repolist() -> None:
    _ensure_initialized()
    rows = db.list_repos()
    if not rows:
        print("No repositories configured.")
        return
    
    print(f"{'ID':<3} {'NAME':<30} {'TYPE':<8} {'UPDATED':<12} {'URL'}")
    print("-" * 80)
    for r in rows:
        src = str(r.get("source_repo_id") or "-")
        raw_date = r.get("last_updated")
        if raw_date and "T" in raw_date:
            updated = raw_date.split("T")[0]
        else:
            updated = raw_date or "Never"
            
        url = r['base_url']
        if len(url) > 40: url = url[:37] + "..."
        print(f"{r['id']:<3} {r['name']:<30} {r['type']:<8} {updated:<12} {url}")


def reposync(names: List[str], all_: bool) -> None:
    """
    Sync specified repository names, or all if --all.
    REVERTED TO SEQUENTIAL: To ensure NTLM/Proxy Auth stability.
    """
    _ensure_initialized()
    
    if all_:
        repos = db.list_repos()
    else:
        # ... (same name resolution logic) ...
        repos = []
        for n in names:
            # ... (same logic) ...
            r = db.get_repo(n)
            if r: repos.append(r)

    if not repos:
        print("No repositories to sync.")
        return

    print(f"Starting sync for {len(repos)} repositories...")
    
    # SEQUENTIAL LOOP (Proxy Safe)
    for repo in repos:
        try:
            print(f"Syncing repo '{repo['name']}'...")
            metadata.sync_repo(repo)
            print(f"[{Colors.FG_GREEN}OK{Colors.RESET}] {repo['name']}")
        except Exception as e:
            _logger.exception("Failed to sync %s", repo["name"])
            print(f"[{Colors.FG_RED}FAIL{Colors.RESET}] {repo['name']}: {e}")


def repodel(names: List[str], all_: bool, force: bool) -> None:
    _ensure_initialized()
    if all_:
        repos = db.list_repos()
        for r in repos:
            db.delete_repo(r["id"])
            print(f"Deleted {r['name']}")
        return

    if not names:
        print("No repository names provided.")
        return

    for n in names:
        r = db.get_repo(n)
        if not r:
            if force:
                print(f"Repository {n} not found; skipping (force).")
                continue
            else:
                print(f"Repository {n} not found.")
                continue
        db.delete_repo(r["id"])
        print(f"Deleted repository {n}")


# -------------------------
# Package queries
# -------------------------
def search(patterns: List[str], repo: List[str] = None, showduplicates: bool = False) -> None:
    _ensure_initialized()
    repoids = _resolve_repo_names_to_ids(repo) if repo else None

    all_results: List[Dict[str, Any]] = []
    for pat in patterns:
        all_results.extend(db.search_packages(pat, repo_filter=repoids))

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

    results.sort(key=lambda x: (x['name'], NEVRA.from_row(x)))

    for pat in patterns:
        needle = pat.replace("*", "").lower()
        name_summary, summary_only, name_only = [], [], []

        for r in results:
            name = r.get("name", "") or ""
            summary = r.get("summary", "") or ""

            nl = name.lower()
            sl = summary.lower()

            match_name = needle in nl if needle else False
            match_summary = needle in sl if needle else False

            if not (match_name or match_summary):
                continue

            nevra_obj = NEVRA.from_row(r)
            nevra_str = str(nevra_obj)

            disp_summary = highlight_match(summary, pat.replace("*", "")) if match_summary else summary
            nevra_disp = highlight_name_in_nevra(nevra_str, name, pat.replace("*", "")) if match_name else nevra_str

            line = f"{nevra_disp} : {disp_summary}"

            if match_name and match_summary:
                name_summary.append(line)
            elif match_summary:
                summary_only.append(line)
            elif match_name:
                name_only.append(line)

        if name_summary:
            print_delimiter(f"Name & Summary Matched: {pat}")
            for line in name_summary: print(line)
        if summary_only:
            print_delimiter(f"Summary Matched: {pat}")
            for line in summary_only: print(line)
        if name_only:
            print_delimiter(f"Name Matched: {pat}")
            for line in name_only: print(line)


def info(pattern: str, repo: Optional[List[str]]) -> None:
    _ensure_initialized()
    repo_ids = _resolve_repo_names_to_ids(repo)
    rows = db.search_packages(pattern, repo_filter=repo_ids)
    if not rows:
        print("No packages match.")
        return

    best = max(rows, key=lambda row: NEVRA.from_row(row))
    row = best
    nevra = NEVRA.from_row(row)
    
    print_delimiter("Package Info")
    print(f"Name        : {row['name']}")
    print(f"Arch        : {row.get('arch')}")
    print(f"Epoch       : {row.get('epoch') or 0}")
    print(f"Version     : {row.get('version')}")
    print(f"Release     : {row.get('release')}")
    print(f"Size        : {row.get('size_package', 0) // 1024} k")
    print(f"Repo        : {row.get('repo_id')}")
    print(f"Summary     : {row.get('summary')}")
    print(f"URL         : {row.get('url') or ''}")
    print("-" * 40)
    print(f"Description :\n{row.get('description') or ''}")


# -------------------------
# Solver Logic (RHEL-Grade)
# -------------------------
def _solve_dependencies(
    packages: List[str], 
    repo_ids: Optional[List[int]], 
    recursive: bool = True, 
    weakdeps: bool = False,
    arch: Optional[str] = None
) -> Set[int]:
    """
    Core solver: Returns a Set of pkgKeys (integers) covering requested packages + dependencies.
    Implements: Exact Match Priority, Arch Scoring, Version Checks.
    """
    provides = db.provides_map()
    requires = db.requires_map()

    # Define Arch Preference
    PREFERRED_ARCH = ["x86_64", "noarch", "i686"]
    
    def check_version(req: Dict[str, Any], cand_row: Dict[str, Any]) -> bool:
        """Returns True if candidate satisfies the requirement's version constraint."""
        flags = req.get("flags")
        if not flags:
            return True 

        # Parse Versions
        r_evr = (req.get("epoch") or "0", req.get("version"), req.get("release"))
        c_epoch = str(cand_row.get("epoch") or "0")
        c_ver = cand_row.get("version")
        c_rel = cand_row.get("release")

        # Compare using rpmvercmp
        cmp = rpmvercmp(c_epoch, r_evr[0])
        if cmp == 0:
            cmp = rpmvercmp(c_ver, r_evr[1])
            if cmp == 0 and r_evr[2]:
                cmp = rpmvercmp(c_rel, r_evr[2])

        if flags in ("EQ", "="): return cmp == 0
        elif flags in ("LT", "<"): return cmp < 0
        elif flags in ("LE", "<="): return cmp <= 0
        elif flags in ("GT", ">"): return cmp > 0
        elif flags in ("GE", ">="): return cmp >= 0
        
        return True 

    def score_candidate(row: Dict[str, Any]) -> Tuple[int, NEVRA]:
        """Score: Arch (High) > Version (High)"""
        a = row["arch"]
        if arch and a == arch: val = 100
        elif a == "x86_64": val = 50
        elif a == "noarch": val = 40
        elif a == "i686": val = 10
        else: val = 0
        return (val, NEVRA.from_row(row))

    def choose_candidate(req: Dict[str, Any]) -> Optional[int]:
        name = req["name"]
        candidates = list(provides.get(name, []))
        if not candidates:
            return None
        
        valid_rows = []
        for pk in candidates:
            p_row = db.get_by_key(pk)
            if not p_row: continue
            
            # Repo Filter
            if repo_ids is not None and p_row["repo_id"] not in repo_ids:
                continue
            
            # Arch Filter (Reject src unless requested)
            if not arch and p_row["arch"] in ("src", "nosrc"):
                continue

            # Version Check
            if not check_version(req, p_row):
                continue
                
            valid_rows.append(p_row)
            
        if not valid_rows:
            return None
            
        # Sort by Score
        valid_rows.sort(key=score_candidate, reverse=True)
        return valid_rows[0]["pkgKey"]

    # --- Solver Loop ---
    queue: List[int] = []
    
    # Initial User Input
    for p in packages:
        # Strict Exact Name Check First
        candidates = db.search_packages(p, repo_filter=repo_ids)
        exact = [r for r in candidates if r["name"] == p]
        
        target_set = exact if exact else candidates
        
        if target_set:
            if arch:
                target_set = [r for r in target_set if r["arch"] == arch]
                
            if target_set:
                target_set.sort(key=score_candidate, reverse=True)
                queue.append(target_set[0]["pkgKey"])
                continue

        print(f"Warning: Package '{p}' not found.")

    resolved = set()
    while queue:
        pk = queue.pop(0)
        if pk in resolved:
            continue
        resolved.add(pk)
        
        if not recursive:
            continue
            
        reqs = requires.get(pk, [])
        for r_item in reqs:
            if not weakdeps and r_item.get("flags") == "weak":
                continue
            if r_item["name"].startswith("rpmlib("):
                continue

            cand_key = choose_candidate(r_item)
            if cand_key and cand_key not in resolved:
                queue.append(cand_key)

    return resolved


# -------------------------
# Commands using the Solver
# -------------------------
def resolve(
    packages: List[str], repo: Optional[List[str]], weakdeps: bool, recursive: bool, arch: Optional[str]
) -> None:
    _ensure_initialized()
    repo_ids = _resolve_repo_names_to_ids(repo)
    
    print(f"Resolving dependencies for: {', '.join(packages)}...")
    final_keys = _solve_dependencies(packages, repo_ids, recursive, weakdeps, arch)
    
    print_delimiter(f"Transaction Summary ({len(final_keys)} packages)")
    for pk in final_keys:
        row = db.get_by_key(pk)
        print(f"  {NEVRA.from_row(row)}")


def download(
    packages: List[str],
    repo: Optional[List[str]],
    downloaddir: Optional[str],
    destdir: Optional[str],
    resolve: bool,      
    recurse: bool,      
    source: bool,
    urls: bool,
    arch: Optional[str],
) -> None:
    _ensure_initialized()
    target_path_str = destdir or downloaddir or str(_cfg.download_path)
    download_dir = Path(target_path_str)
    repo_ids = _resolve_repo_names_to_ids(repo)

    # 1. SOLVE
    should_resolve = resolve or recurse
    
    if should_resolve:
        print("Calculating dependencies...")
        target_keys = _solve_dependencies(packages, repo_ids, recursive=True, arch=arch)
    else:
        # Exact match only
        target_keys = _solve_dependencies(packages, repo_ids, recursive=False, arch=arch)

    if not target_keys:
        print("Nothing to download.")
        return

    # 2. FETCH
    targets = [db.get_by_key(pk) for pk in target_keys]
    download_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Downloading {len(targets)} packages to '{download_dir}'...")

    for row in targets:
        nevra = NEVRA.from_row(row)
        
        rel_url = row.get("location_href") or row.get("href")
        base = row.get("location_base") or db.get_repo(row["repo_id"])["base_url"]
        full_url = f"{base.rstrip('/')}/{rel_url.lstrip('/')}"

        if urls:
            print(full_url)
            continue
            
        filename = rel_url.split("/")[-1]
        outpath = download_dir / filename
        
        try:
            print(f"Fetching: {filename}")
            downloader.download_to_file(full_url, outpath)
        except Exception as e:
            print(f"[{Colors.FG_RED}FAIL{Colors.RESET}] {filename}: {e}")

    if not urls:
        print("Download complete.")