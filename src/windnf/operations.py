# operations.py
from __future__ import annotations

import fnmatch
import re
import shutil
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

from .config import Config
from .db_manager import DbManager
from .downloader import Downloader
from .logger import Colors, setup_logger
from .metadata_manager import MetadataManager
from .nevra import NEVRA

_logger = setup_logger()


class Operations:
    def __init__(self, config: Config):
        self.cfg = config
        self.db = DbManager(config)
        self.downloader = Downloader(config)
        self.metadata = MetadataManager(config, self.db, self.downloader, max_workers=4)
        _logger.debug(
            "operations initialized with DB=%s, downloader=%s",
            config.db_path,
            config.downloader,
        )

    # --- Utilities ---
    def highlight_match(self, text: str, pattern: str) -> str:
        if not pattern:
            return text
        pattern_re = re.compile(re.escape(pattern), re.IGNORECASE)
        return pattern_re.sub(
            lambda m: f"{Colors.FG_BRIGHT_RED}{Colors.BOLD}{m.group(0)}{Colors.RESET}",
            text,
        )

    def highlight_name_in_nevra(self, nevra_str: str, name: str, pattern: Optional[str]) -> str:
        if not pattern or not name:
            return nevra_str
        highlighted_name = self.highlight_match(name, pattern)
        escaped_name = re.escape(name)
        return re.sub(escaped_name, highlighted_name, nevra_str, count=1, flags=re.IGNORECASE)

    def print_delimiter(self, title: str) -> None:
        width = shutil.get_terminal_size((80, 20)).columns
        delimiter = "="
        title_len = len(title) + 2
        side_len = (width - title_len) // 2
        line = delimiter * side_len + f" {title} " + delimiter * (width - side_len - title_len)
        print(line)

    def _resolve_repo_names_to_ids(self, repo_names: Optional[Sequence[str]]) -> Optional[List[int]]:
        if not repo_names:
            return None
        out: List[int] = []
        for name in repo_names:
            repo = self.db.get_repo(name)
            if not repo:
                raise ValueError(f"Repository not found: {name}")
            out.append(int(repo["id"]))
        return out

    # --- Repository Operations ---
    def repoadd(
        self, name: str, baseurl: str, repomd: str, repo_type: str, source_repo: Optional[str], sync: bool
    ) -> None:
        src_id = None
        if source_repo:
            src = self.db.get_repo(source_repo)
            if not src:
                raise ValueError(f"Source repo not found: {source_repo}")
            src_id = int(src["id"])
        rid = self.db.add_repo(
            name=name,
            base_url=baseurl.rstrip("/"),
            repomd_url=repomd,
            rtype=repo_type,
            source_repo_id=src_id,
        )
        print(f"Repository '{name}' added/updated (id={rid}).")
        if sync:
            repo_row = self.db.get_repo(int(rid))
            if repo_row:
                self.metadata.sync_repo(repo_row)
            else:
                print("Repo created but could not load repository row.")

    def repolink(self, binary_repo: str, source_repo: str) -> None:
        self.db.link_source(binary_repo, source_repo)
        print(f"Linked binary repo '{binary_repo}' -> source repo '{source_repo}'")

    def repolist(self):
        rows = self.db.list_repos()
        if not rows:
            print("No repositories configured.")
            return
        term_w = shutil.get_terminal_size((80, 20)).columns
        spacing = 2
        id_w, type_w, src_w = 4, 6, 12  # make src_w wider for names
        name_w = 12
        min_url_w = 20
        max_url_w = 80
        remaining = term_w - (id_w + name_w + type_w + src_w + spacing * 4)
        url_w = min(max_url_w, max(min_url_w, remaining))

        def trunc(s, w):
            return s if len(s) <= w else s[: w - 1] + "…"

        print(
            f"{'ID':<{id_w}}{' '*spacing}{'Name':<{name_w}}{' '*spacing}"
            f"{'Base URL':<{url_w}}{' '*spacing}{'Type':<{type_w}}{' '*spacing}{'Src':<{src_w}}"
        )
        print("-" * term_w)
        for r in rows:
            src_id = r.get("source_repo_id")
            if src_id:
                src_repo = self.db.get_repo(src_id)
                src_name = src_repo["name"] if src_repo else "-"
            else:
                src_name = "-"
            name, url = r["name"], r["base_url"]
            print(
                f"{r['id']:<{id_w}}{' '*spacing}{trunc(name, name_w):<{name_w}}{' '*spacing}"
                f"{trunc(url, url_w):<{url_w}}{' '*spacing}{r['type']:<{type_w}}{' '*spacing}{trunc(src_name, src_w):<{src_w}}"
            )

    def reposync(self, names: List[str], all_: bool) -> None:
        if all_:
            repos = self.db.list_repos()
        else:
            repos = [r for n in names if (r := self.db.get_repo(n)) is not None]
        if not repos:
            print("No repositories to sync.")
            return
        for r in repos:
            print(f"Syncing {r['name']}...")
            try:
                self.metadata.sync_repo(r)
            except Exception as e:
                _logger.exception("Failed to sync %s: %s", r["name"], e)
                print(f"Failed to sync {r['name']}: {e}")
            else:
                print(f"Synced {r['name']}")

    def repodel(self, names: Optional[List[str]] = None, all_: bool = False, force: bool = False) -> None:
        names = names or []
        if all_:
            for repo in self.db.list_repos():
                if force or input(f"Delete {repo['name']}? [y/N]: ").lower() == "y":
                    self.db.delete_repo(repo["id"])
                    print(f"Deleted {repo['name']}")
            return
        for identifier in names:
            repo = self.db.get_repo(identifier)
            if not repo:
                print(f"Repository {identifier} not found.")
                continue
            if force or input(f"Delete repository {repo['name']}? [y/N]: ").lower() == "y":
                self.db.delete_repo(repo["id"])
                print(f"Deleted repository {repo['name']}")

    # --- Package Search / Info ---
    def search(self, patterns: List[str], repo: Optional[List[str]] = None, showduplicates: bool = False) -> None:
        repoids = self._resolve_repo_names_to_ids(repo) if repo else None
        all_results: List[Dict[str, Any]] = []
        for pat in patterns:
            all_results.extend(self.db.search_packages(pat, repo_filter=repoids, exact=False))
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
                match_name = fnmatch.fnmatchcase(name_lc, pat_lc) if is_wildcard else pat_lc in name_lc
                match_summary = fnmatch.fnmatchcase(summary_lc, pat_lc) if is_wildcard else pat_lc in summary_lc
                if not (match_name or match_summary):
                    continue
                nevra_str = str(r["_nevra"])
                disp_summary = self.highlight_match(summary, pat) if match_summary and not is_wildcard else summary
                nevra_disp = (
                    self.highlight_name_in_nevra(nevra_str, name, pat) if match_name and not is_wildcard else nevra_str
                )
                line = f"{nevra_disp} : {disp_summary}"
                if match_name and match_summary:
                    name_summary.append(line)
                elif match_summary:
                    summary_only.append(line)
                elif match_name:
                    name_only.append(line)

            if name_summary:
                self.print_delimiter(f"Name & Summary Matched: {pat}")
                for line in name_summary:
                    print(line)
            if summary_only:
                self.print_delimiter(f"Summary Matched: {pat}")
                for line in summary_only:
                    print(line)
            if name_only:
                self.print_delimiter(f"Name Matched: {pat}")
                for line in name_only:
                    print(line)

    def info(self, pattern: str, repo: Optional[List[str]] = None) -> None:
        repo_ids = self._resolve_repo_names_to_ids(repo) if repo else None
        rows = self.db.search_packages(pattern, repo_filter=repo_ids, exact=True)
        if not rows:
            print("No packages match.")
            return

        best_row = max(rows, key=lambda row: NEVRA.from_row(row))
        nevra = NEVRA.from_row(best_row)

        print(f"Package: {nevra}")
        repo_name = (
            self.db.get_repo(best_row["repo_id"])["name"] if best_row.get("repo_id") else best_row.get("repo_id")
        )
        print(f" Repo: {repo_name}")
        print(f" Arch: {best_row.get('arch')}")
        print(f" Summary: {best_row.get('summary')}")
        print(f" URL: {best_row.get('url') or ''}")

    # --- Dependency Resolver ---
    def _resolve_dependencies(
        self,
        packages: List[str],
        repo: Optional[List[str]] = None,
        weakdeps: bool = False,
        recursive: bool = False,
        arch: Optional[str] = None,
    ) -> Dict[str, Any]:
        repo_ids = self._resolve_repo_names_to_ids(repo) if repo else None

        to_resolve: List[Dict[str, Any]] = []
        for pat in packages:
            rows = self.db.search_packages(pat, repo_filter=repo_ids, exact=True)
            if not rows:
                continue
            best_row = max(rows, key=lambda r: NEVRA.from_row(r))
            to_resolve.append(best_row)

        if not to_resolve:
            return {"resolved_rows": [], "dep_map": {}, "unsatisfied": set()}

        provides_map = self.db.provides_map(repo_filter=repo_ids)
        requires_map = self.db.requires_map()

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
                        prov_row = self.db.get_by_key(pKey, repo_filter=repo_ids)
                        if not prov_row:
                            continue
                        dep_map[pkgKey].append(prov_row)
                        if recursive and pKey not in resolved_keys:
                            stack.append(prov_row)
                else:
                    unsatisfied_dependencies.add(req_name)

        resolved_rows = [self.db.get_by_key(k, repo_filter=repo_ids) for k in resolved_keys]
        resolved_rows = [r for r in resolved_rows if r is not None]

        return {"resolved_rows": resolved_rows, "dep_map": dep_map, "unsatisfied": unsatisfied_dependencies}

    def resolve(
        self,
        packages: List[str],
        repo: Optional[List[str]] = None,
        weakdeps: bool = False,
        recursive: bool = False,
        arch: Optional[str] = None,
        verbose: bool = False,
    ) -> None:
        result = self._resolve_dependencies(packages, repo, weakdeps, recursive, arch)
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
                self.print_delimiter(f"Package: {pkg_nevra}")
                self.info(pkg_row["name"], repo)
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

    # --- Download Packages ---
    def download(
        self,
        packages: List[str],
        repo: Optional[List[str]] = None,
        downloaddir: Optional[str] = None,
        destdir: Optional[str] = None,
        resolve_flag: bool = False,
        recurse: bool = False,
        source: bool = False,
        urls: bool = False,
        arch: Optional[str] = None,
    ) -> None:
        if resolve_flag or recurse:
            result = self._resolve_dependencies(packages, repo=repo, weakdeps=False, recursive=recurse, arch=arch)
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
            repo_ids = self._resolve_repo_names_to_ids(repo)
            for p in packages:
                try:
                    nv = NEVRA.parse(p)
                except Exception:
                    nv = None
                rows = self.db.search_packages(str(nv) if nv else p, repo_filter=repo_ids, exact=True)
                if not rows:
                    print(f"No match for {p}")
                    continue
                best = max(rows, key=lambda r: NEVRA.from_row(r))
                targets_list.append(best)

        if not targets_list:
            print("No packages selected for download.")
            return

        download_dir = Path(downloaddir) if downloaddir else self.cfg.download_path
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
            repo_row = self.db.get_repo(int(row["repo_id"]))
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
                src_rows = self.db.search_packages(row["rpm_sourcerpm"], repo_filter=None, exact=True)
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
                    if hasattr(self.downloader, "download_to_file"):
                        self.downloader.download_to_file(url, outpath)
                    else:
                        data = self.downloader.download_to_memory(url)
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
