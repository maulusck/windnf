# operations.py
from __future__ import annotations

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
    """Highlight all case-insensitive occurrences of pattern in text using ANSI colors."""
    if not pattern:
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
    width = shutil.get_terminal_size((80, 20)).columns
    delimiter = "="
    title_len = len(title) + 2
    side_len = (width - title_len) // 2
    line = delimiter * side_len + f" {title} " + delimiter * (width - side_len - title_len)
    print(line)


# -------------------------
# Repository commands
# -------------------------
def repoadd(name: str, baseurl: str, repomd: str, repo_type: str, source_repo: Optional[str]) -> None:
    """
    Add (or update) repository and sync.
    CLI signature: name, baseurl, --repomd, --type, --source-repo
    """
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
    """
    Link a binary repo to a source repo.
    """
    db.link_source(binary_repo, source_repo)
    print(f"Linked binary repo '{binary_repo}' -> source repo '{source_repo}'")


def repolist() -> None:
    """
    List all configured repositories.
    """
    rows = db.list_repos()
    if not rows:
        print("No repositories configured.")
        return
    for r in rows:
        src = r.get("source_repo_id") or "-"
        print(f"{r['id']:>3} {r['name']:30} {r['base_url']:40} type={r['type']} src_id={src}")


def reposync(names: List[str], all_: bool) -> None:
    """
    Sync specified repository names, or all if --all.
    CLI: names (list) and all_ boolean
    """
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
    """
    Delete repositories by name or ID.
    """
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


# -------------------------
# Package queries
# -------------------------
import fnmatch
from typing import Any, Dict, List


def search(patterns: List[str], repo: List[str] = None, showduplicates: bool = False) -> None:
    """
    Search for packages by patterns.

    - If showduplicates is False, only the latest NEVRA per package name is shown.
    - Results grouped by match type: Name & Summary, Summary-only, Name-only.
    - Highlights only when pattern has no wildcard (*).
    """
    repoids = _resolve_repo_names_to_ids(repo) if repo else None

    all_results: List[Dict[str, Any]] = []
    for pat in patterns:
        all_results.extend(db.search_packages(pat, repo_filter=repoids))

    if not all_results:
        print("No packages found.")
        return

    # Keep only latest per package name if duplicates not desired
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

    # Precompute lowercase and NEVRA objects
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
                # Wildcard match
                match_name = fnmatch.fnmatchcase(name_lc, pat_lc)
                match_summary = fnmatch.fnmatchcase(summary_lc, pat_lc)
            else:
                # Simple substring match (case-insensitive)
                match_name = pat_lc in name_lc
                match_summary = pat_lc in summary_lc

            if not (match_name or match_summary):
                continue

            nevra_str = str(r["_nevra"])

            # Only highlight for non-wildcard patterns
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


def info(pattern: str, repo: Optional[List[str]]) -> None:
    """
    Show detailed info for a package matching pattern (NEVRA or name).
    """
    repo_ids = _resolve_repo_names_to_ids(repo)
    rows = db.search_packages(pattern, repo_filter=repo_ids)
    if not rows:
        print("No packages match.")
        return

    # If multiple matches, choose the newest per NEVRA ordering
    best = max(rows, key=lambda row: NEVRA.from_row(row))
    row = best
    nevra = NEVRA.from_row(row)
    print(f"Package: {nevra}")
    print(f" Repo: {row.get('repo_id')}")
    print(f" Arch: {row.get('arch')}")
    print(f" Summary: {row.get('summary')}")
    print(f" URL: {row.get('url') or ''}")

    # fetch provides/requires
    provs = [
        dict(x)
        for x in db.conn.execute(
            "SELECT name,flags,epoch,version,release FROM provides WHERE pkgKey=?", (row["pkgKey"],)
        ).fetchall()
    ]
    reqs = [
        dict(x)
        for x in db.conn.execute(
            "SELECT name,flags,epoch,version,release,pre FROM requires WHERE pkgKey=?", (row["pkgKey"],)
        ).fetchall()
    ]

    if provs:
        print(" Provides:")
        for p in provs:
            print(f"  - {p['name']}")

    if reqs:
        print(" Requires:")
        for r in reqs:
            pre = " (pre)" if r.get("pre") else ""
            print(f"  - {r['name']}{pre}")


# -------------------------
# Resolver (heuristic)
# -------------------------
def resolve(
    packages: List[str], repo: Optional[List[str]], weakdeps: bool, recursive: bool, arch: Optional[str]
) -> None:
    """
    Resolve packages to concrete pkgKey set using a heuristic.
    Prints the list of NEVRAs resolved.
    """
    repo_ids = _resolve_repo_names_to_ids(repo)
    provides = db.provides_map()
    requires = db.requires_map()

    # helper to choose best candidate for a requirement dict
    def choose_candidate(req: Dict[str, Any]):
        name = req["name"]
        candidates = list(provides.get(name, []))
        if not candidates:
            return None
        # apply repo and arch filters
        if repo_ids is not None:
            candidates = [pk for pk in candidates if db.get_by_key(pk)["repo_id"] in repo_ids]
        if arch is not None:
            candidates = [pk for pk in candidates if db.get_by_key(pk)["arch"] == arch]
        if not candidates:
            return None
        # prefer exact epoch/version/release if specified
        for pk in candidates:
            prow = db.get_by_key(pk)
            if req.get("epoch") and str(prow.get("epoch") or "0") != str(req.get("epoch")):
                continue
            if req.get("version") and prow.get("version") != req.get("version"):
                continue
            if req.get("release") and prow.get("release") != req.get("release"):
                continue
            return pk
        # otherwise pick highest NEVRA
        best = max(candidates, key=lambda k: NEVRA.from_row(db.get_by_key(k)))
        return best

    # build initial queue from provided package patterns
    queue: List[int] = []
    for p in packages:
        try:
            nv = NEVRA.parse(p)
        except Exception:
            nv = None
        if nv:
            # try exact lookup
            rows = db.search_packages(str(nv))
            if rows:
                queue.append(rows[0]["pkgKey"])
                continue
            # fallback newest by name
            rows = db.search_packages(nv.name, repo_filter=repo_ids)
            if rows:
                best = max(rows, key=lambda r: NEVRA.from_row(r))
                queue.append(best["pkgKey"])
                continue
        else:
            # treat as name
            rows = db.search_packages(p, repo_filter=repo_ids)
            if rows:
                best = max(rows, key=lambda r: NEVRA.from_row(r))
                queue.append(best["pkgKey"])

    resolved = set()
    while queue:
        pk = queue.pop(0)
        if pk in resolved:
            continue
        resolved.add(pk)
        if not recursive:
            continue
        reqs = requires.get(pk, [])
        for req in reqs:
            if not weakdeps and req.get("flags") == "weak":
                continue
            cand = choose_candidate(req)
            if cand and cand not in resolved:
                queue.append(cand)

    # print resolved NEVRAs
    out = [NEVRA.from_row(db.get_by_key(pk)) for pk in resolved]
    print("Resolved:")
    for n in out:
        print(f"  {n}")


# -------------------------
# Downloading
# -------------------------
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
    Download packages or print URLs.
    - packages: list of package patterns or NEVRA
    - repo: list of repo names (optional)
    - downloaddir/destdir: directories
    - resolve_flag: whether to resolve dependencies (ignored; we do simple resolution if requested)
    - recurse: same as resolve for now
    - source: include SRPMs
    - urls: print URLs only
    - arch: architecture filter
    """
    repo_ids = _resolve_repo_names_to_ids(repo)

    # resolve to concrete pkgKeys
    # reuse resolve() heuristic but return pkg keys
    # simplified: if resolve_flag or recurse -> call resolve-like logic to collect set
    # else expand patterns to best-matching packages
    selected_pkgkeys = set()

    if resolve_flag or recurse:
        # call the resolver implemented above but return the set
        # little helper to reuse above code: replicate minimal logic
        provides = db.provides_map()
        requires = db.requires_map()

        def pick_for_name(name: str):
            cands = list(provides.get(name, []))
            if repo_ids is not None:
                cands = [pk for pk in cands if db.get_by_key(pk)["repo_id"] in repo_ids]
            if arch is not None:
                cands = [pk for pk in cands if db.get_by_key(pk)["arch"] == arch]
            if not cands:
                return None
            best = max(cands, key=lambda k: NEVRA.from_row(db.get_by_key(k)))
            return best

        # seeds
        queue = []
        for p in packages:
            try:
                nv = NEVRA.parse(p)
            except Exception:
                nv = None
            if nv:
                rows = db.search_packages(str(nv), repo_filter=repo_ids)
                if rows:
                    queue.append(rows[0]["pkgKey"])
                else:
                    rows = db.search_packages(nv.name, repo_filter=repo_ids)
                    if rows:
                        best = max(rows, key=lambda r: NEVRA.from_row(r))
                        queue.append(best["pkgKey"])
            else:
                rows = db.search_packages(p, repo_filter=repo_ids)
                if rows:
                    best = max(rows, key=lambda r: NEVRA.from_row(r))
                    queue.append(best["pkgKey"])

        while queue:
            pk = queue.pop(0)
            if pk in selected_pkgkeys:
                continue
            selected_pkgkeys.add(pk)
            if not recurse:
                continue
            for req in requires.get(pk, []):
                cand = pick_for_name(req["name"])
                if cand and cand not in selected_pkgkeys:
                    queue.append(cand)
    else:
        # no resolution: just pick best match for each pattern
        for p in packages:
            try:
                nv = NEVRA.parse(p)
            except Exception:
                nv = None
            rows = db.search_packages(str(nv) if nv else p, repo_filter=repo_ids)
            if not rows:
                print(f"No match for {p}")
                continue
            best = max(rows, key=lambda r: NEVRA.from_row(r))
            selected_pkgkeys.add(best["pkgKey"])

    # Now we have selected_pkgkeys; build list of NEVRA objects and URLs
    targets = [db.get_by_key(pk) for pk in selected_pkgkeys]
    targets = [t for t in targets if t is not None]

    if not targets:
        print("No packages selected for download.")
        return

    # build URL function
    def build_urls_for_row(row: Dict[str, Any]) -> List[str]:
        urls = []
        # prefer explicit 'url' column
        if row.get("url"):
            urls.append(row["url"])
        # try location_base + location_href if present
        lb = row.get("location_base") or row.get("locationbase") or row.get("location_base_url")
        lh = row.get("location_href") or row.get("locationhref") or row.get("href")
        if lb and lh:
            urls.append(f"{lb.rstrip('/')}/{lh.lstrip('/')}")
        # fallback to repo base_url + href
        repo_row = db.get_repo(int(row["repo_id"]))
        if repo_row and lh:
            urls.append(f"{repo_row['base_url'].rstrip('/')}/{lh.lstrip('/')}")
        return urls

    # if urls flag, print and return
    if urls:
        for row in targets:
            nevra = NEVRA.from_row(row)
            ulist = build_urls_for_row(row)
            if not ulist:
                print(f"{nevra} -> no URL available")
            else:
                for u in ulist:
                    print(u)
        return

    # else perform downloads to downloaddir (or config.download_path) and optionally destdir
    download_dir = Path(downloaddir) if downloaddir else _cfg.download_path
    dest_dir = Path(destdir) if destdir else None
    download_dir.mkdir(parents=True, exist_ok=True)
    if dest_dir:
        dest_dir.mkdir(parents=True, exist_ok=True)

    for row in targets:
        nevra = NEVRA.from_row(row)
        urls = build_urls_for_row(row)
        if not urls:
            print(f"Skipping {nevra}: no URL available")
            continue
        url = urls[0]
        filename = url.split("/")[-1] or f"{nevra.to_nvra()}.rpm"
        outpath = download_dir / filename
        try:
            # If downloader has download_to_file use it; otherwise write bytes
            if hasattr(downloader, "download_to_file"):
                downloader.download_to_file(url, outpath)
            else:
                data = downloader.download_to_memory(url)
                with open(outpath, "wb") as fh:
                    fh.write(data)
            print(f"Downloaded {nevra} -> {outpath}")
            if dest_dir:
                # copy/move to dest_dir (simple copy)
                final = dest_dir / filename
                # use os.replace for atomic move
                try:
                    import shutil

                    shutil.copy2(outpath, final)
                    print(f"Copied to {final}")
                except Exception as e:
                    print(f"Failed to copy to {final}: {e}")
        except Exception as e:
            _logger.exception("Download failed for %s: %s", nevra, e)
            print(f"Failed to download {nevra}: {e}")
