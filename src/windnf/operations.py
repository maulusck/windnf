from __future__ import annotations

import fnmatch
import logging
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

from .config import Config
from .db_manager import DbManager
from .downloader import Downloader
from .logger import Colors
from .metadata_manager import MetadataManager
from .nevra import NEVRA

log = logging.getLogger(__name__)


class Operations:
    def __init__(self, config: Config):
        self.cfg = config
        self.db = DbManager(config)
        self.downloader = Downloader(config)
        self.metadata = MetadataManager(config, self.db, self.downloader, max_workers=4)
        log.debug(
            "Operations initialized with DB=%s, downloader=%s",
            config.db_path,
            config.downloader,
        )

    def highlight_match(self, text: str, pattern: str) -> str:
        if not pattern:
            return text
        regex = re.compile(re.escape(pattern), re.IGNORECASE)
        return regex.sub(lambda m: f"{Colors.FG_BRIGHT_RED}{Colors.BOLD}{m.group(0)}{Colors.RESET}", text)

    def highlight_name_in_nevra(self, nevra_str: str, name: str, pattern: Optional[str]) -> str:
        if not pattern or not name:
            return nevra_str
        highlighted_name = self.highlight_match(name, pattern)
        escaped_name = re.escape(name)
        return re.sub(escaped_name, highlighted_name, nevra_str, count=1, flags=re.IGNORECASE)

    def print_delimiter(self, title: str = "") -> None:
        width = shutil.get_terminal_size((80, 20)).columns
        line = f" {title} ".center(width, "=") if title else "=" * width
        print(line)

    def _resolve_repo_names_to_ids(self, repo_names: Optional[Sequence[str]]) -> Optional[List[int]]:
        if not repo_names:
            return None
        out: List[int] = []
        for name in repo_names:
            repo = self.db.get_repo(name)
            if not repo:
                log.error("Repository not found: %s", name)
                raise ValueError(f"Repository not found: {name}")
            out.append(int(repo["id"]))
        return out

    def repoadd(
        self, name: str, baseurl: str, repomd: str, repo_type: str, source_repo: Optional[str], sync: bool
    ) -> None:
        src_id = None
        if source_repo:
            src = self.db.get_repo(source_repo)
            if not src:
                log.error("Source repo not found: %s", source_repo)
                raise ValueError(f"Source repo not found: {source_repo}")
            src_id = int(src["id"])
        rid = self.db.add_repo(
            name=name,
            base_url=baseurl.rstrip("/"),
            repomd_url=repomd,
            rtype=repo_type,
            source_repo_id=src_id,
        )
        log.info("Repository '%s' added/updated (id=%s)", name, rid)
        if sync:
            self.reposync([name], all_=False)

    def repolink(self, binary_repo: str, source_repo: str) -> None:
        try:
            self.db.link_source(binary_repo, source_repo)
            log.info(f"Successfully linked binary repo '{binary_repo}' -> source repo '{source_repo}'")
        except ValueError as e:
            log.error(f"Error linking repositories: {str(e)}")
            raise

    def repolist(self) -> None:
        rows = self.db.list_repos()
        if not rows:
            log.info("No repositories configured.")
            return
        term_w = shutil.get_terminal_size((80, 20)).columns
        spacing = 2
        id_w, type_w, last_w = 4, 6, 30
        name_w, src_w = 15, 15
        min_url_w, max_url_w = 20, 80
        used_width = id_w + name_w + type_w + src_w + last_w + spacing * 5
        remaining = term_w - used_width
        url_w = min(max_url_w, max(min_url_w, remaining))
        total_w = id_w + name_w + type_w + src_w + last_w + url_w + spacing * 5
        if total_w > term_w:
            shrink_ratio = (term_w - spacing * 5) / (id_w + name_w + type_w + src_w + last_w + url_w)
            id_w = max(2, int(id_w * shrink_ratio))
            name_w = max(6, int(name_w * shrink_ratio))
            type_w = max(4, int(type_w * shrink_ratio))
            src_w = max(6, int(src_w * shrink_ratio))
            last_w = max(6, int(last_w * shrink_ratio))
            url_w = max(10, int(url_w * shrink_ratio))

        def trunc(s, w):
            return s if s and len(s) <= w else (s[: w - 1] + "…") if s else "-"

        header = (
            f"{'ID':<{id_w}}{' '*spacing}{'Name':<{name_w}}{' '*spacing}"
            f"{'Base URL':<{url_w}}{' '*spacing}{'Type':<{type_w}}{' '*spacing}"
            f"{'Src':<{src_w}}{' '*spacing}{'Last Synced':<{last_w}}"
        )
        print(header)
        print("-" * term_w)
        for r in rows:
            src_id = r.get("source_repo_id")
            src_name = "-"
            if src_id:
                src_repo = self.db.get_repo(src_id)
                src_name = src_repo["name"] if src_repo else "-"
            last_synced = r.get("last_updated") or "-"
            name, url = r["name"], r["base_url"]
            print(
                f"{trunc(str(r['id']), id_w):<{id_w}}{' '*spacing}{trunc(name, name_w):<{name_w}}{' '*spacing}"
                f"{trunc(url, url_w):<{url_w}}{' '*spacing}{trunc(r['type'], type_w):<{type_w}}{' '*spacing}"
                f"{trunc(src_name, src_w):<{src_w}}{' '*spacing}{trunc(last_synced, last_w):<{last_w}}"
            )

    def reposync(self, names: List[str], all_: bool) -> None:
        repos = self.db.list_repos() if all_ else [r for n in names if (r := self.db.get_repo(n)) is not None]

        if not repos:
            log.info("No repositories to sync.")
            return

        for r in repos:
            name = r["name"]
            log.info("Starting sync for repository '%s'", name)
            try:
                self.metadata.sync_repo(r)
            except RuntimeError as e:
                log.error("Failed to sync repository '%s': %s", name, e)
            else:
                log.info("Successfully synced repository '%s'", name)

    def repodel(self, names: Optional[List[str]] = None, all_: bool = False, force: bool = False) -> None:
        names = names or []
        repos_to_delete = self.db.list_repos() if all_ else [self.db.get_repo(n) for n in names if self.db.get_repo(n)]

        if not repos_to_delete:
            log.info("No repositories found for deletion.")
            return

        for repo in repos_to_delete:
            if repo is None:
                continue
            name = repo["name"]
            proceed = force or input(f"Delete repository {name}? [y/N]: ").lower() == "y"
            if proceed:
                self.db.delete_repo(repo["id"])
                log.info("Deleted repository '%s'", name)
            else:
                log.info("Skipped deletion of repository '%s'", name)

    def search(self, patterns: List[str], repo: Optional[List[str]] = None, showduplicates: bool = False) -> None:
        repo_ids = self._resolve_repo_names_to_ids(repo) if repo else None
        all_results: List[Dict[str, Any]] = []

        for pat in patterns:
            results = self.db.search_packages(pat, repo_filter=repo_ids, exact=False)
            if results:
                all_results.extend(results)
            else:
                log.info("No packages found for pattern: %s", pat)

        if not all_results:
            log.info("No packages matched any patterns.")
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

    def info(
        self,
        packages: List[str],
        repo: Optional[List[str]] = None,
        weakdeps: bool = False,
        recursive: bool = False,
        arch: Optional[str] = None,
        verbose: bool = False,
    ) -> None:
        repo_ids = self._resolve_repo_names_to_ids(repo) if repo else None
        for pat in packages:
            rows = self.db.search_packages(pat, repo_filter=repo_ids, exact=True)
            if not rows:
                log.info("No packages match pattern: %s", pat)
                continue
            best_row = max(rows, key=lambda row: NEVRA.from_row(row))
            nevra = NEVRA.from_row(best_row)
            repo_name = self.db.get_repo(best_row["repo_id"])["name"] if best_row.get("repo_id") else "<unknown>"
            self.print_delimiter(f"Package Information for {pat}")
            print(f"Package: {nevra}")
            print(f" Repo: {repo_name}")
            print(f" Arch: {best_row.get('arch')}")
            print(f" Summary: {best_row.get('summary')}")
            print(f" URL: {best_row.get('url') or ''}")
            self.print_delimiter()

    def _resolve_dependencies(
        self,
        packages: List[str],
        repo: Optional[List[str]] = None,
        weakdeps: bool = False,
        recursive: Optional[int] = None,
        arch: Optional[str] = None,
    ) -> Dict[str, Any]:

        repo_ids = self._resolve_repo_names_to_ids(repo) if repo else None

        roots: List[Dict[str, Any]] = []
        for pat in packages:
            rows = self.db.search_packages(pat, repo_filter=repo_ids, exact=True)
            if rows:
                roots.append(max(rows, key=lambda r: NEVRA.from_row(r)))

        if not roots:
            return {
                "roots": [],
                "packages": {},
                "deplist": {},
                "levels": {},
                "unsatisfied": set(),
            }

        provides_map = self.db.provides_map(repo_filter=repo_ids)
        requires_map = self.db.requires_map()

        packages_map: Dict[int, Dict[str, Any]] = {}
        deplist: Dict[int, List[Dict[str, Any]]] = {}
        levels: Dict[int, int] = {}
        unsatisfied: Set[str] = set()

        stack: List[tuple[Dict[str, Any], Optional[int], int]] = []
        for r in roots:
            stack.append((r, recursive, 0))

        while stack:
            pkg_row, depth, level = stack.pop()
            pkgKey = pkg_row["pkgKey"]

            if pkgKey in packages_map:
                continue

            packages_map[pkgKey] = pkg_row
            levels[pkgKey] = level
            deplist[pkgKey] = []

            if depth == 0:
                continue

            for req in sorted(requires_map.get(pkgKey, []), key=lambda r: r["name"]):
                cap = req["name"]
                provider_keys = provides_map.get(cap)

                if not provider_keys:
                    unsatisfied.add(cap)
                    continue

                providers: List[Dict[str, Any]] = []
                for pkey in provider_keys:
                    prow = self.db.get_by_key(pkey, repo_filter=repo_ids)
                    if prow:
                        providers.append(prow)

                if not providers:
                    unsatisfied.add(cap)
                    continue

                providers.sort(key=lambda r: NEVRA.from_row(r), reverse=True)
                chosen = providers[0]

                deplist[pkgKey].append(
                    {
                        "require": cap,
                        "providers": providers,
                        "chosen": chosen,
                    }
                )

                if recursive is not None:
                    next_depth = None if depth is None or depth < 0 else depth - 1
                    stack.append((chosen, next_depth, level + 1))

        return {
            "roots": [r["pkgKey"] for r in roots],
            "packages": packages_map,
            "deplist": deplist,
            "levels": levels,
            "unsatisfied": unsatisfied,
        }

    def resolve(
        self,
        packages: List[str],
        repo: Optional[List[str]] = None,
        weakdeps: bool = False,
        recursive: Optional[int] = None,
        arch: Optional[str] = None,
        verbose: bool = False,
    ) -> None:

        res = self._resolve_dependencies(packages, repo, weakdeps, recursive, arch)

        if not res["roots"]:
            log.info("No packages resolved.")
            return

        pkgs = res["packages"]
        deplist = res["deplist"]
        levels = res["levels"]

        if recursive is not None:
            log.info(
                f"{Colors.FG_BRIGHT_MAGENTA}{Colors.DIM}(note: recursive deplist is a windnf extension){Colors.RESET}\n"
            )

        for root_key in res["roots"]:
            root = pkgs[root_key]
            log.info(f"package: {Colors.FG_BRIGHT_CYAN}{Colors.BOLD}{NEVRA.from_row(root)}{Colors.RESET}")

            deps = deplist.get(root_key, [])
            if not deps:
                log.info(f" {Colors.DIM}<no dependencies>{Colors.RESET}\n")
                continue

            for dep in deps:
                log.info(f" dependency: {Colors.FG_YELLOW}{dep['require']}{Colors.RESET}")
                for prov in dep["providers"]:
                    log.info(f"  provider: {Colors.FG_GREEN}{NEVRA.from_row(prov)}{Colors.RESET}")
                    if verbose:
                        repo_row = self.db.get_repo(prov["repo_id"])
                        if repo_row:
                            log.info(f"   repo: {Colors.DIM}{repo_row['name']}{Colors.RESET}")
            log.info("")

        if recursive is not None:
            for pkgKey, lvl in sorted(levels.items(), key=lambda x: x[1]):
                if lvl == 0:
                    continue
                pkg = pkgs[pkgKey]
                log.info(f"{Colors.FG_BRIGHT_MAGENTA}{Colors.DIM}[level {lvl}]{Colors.RESET}")
                log.info(f"package: {Colors.FG_BRIGHT_CYAN}{Colors.BOLD}{NEVRA.from_row(pkg)}{Colors.RESET}")
                for dep in deplist.get(pkgKey, []):
                    log.info(f" dependency: {Colors.FG_YELLOW}{dep['require']}{Colors.RESET}")
                    for prov in dep["providers"]:
                        log.info(f"  provider: {Colors.FG_GREEN}{NEVRA.from_row(prov)}{Colors.RESET}")
                        if verbose:
                            repo_row = self.db.get_repo(prov["repo_id"])
                            if repo_row:
                                log.info(f"   repo: {Colors.DIM}{repo_row['name']}{Colors.RESET}")
                log.info("")

        if res["unsatisfied"]:
            log.warning("unsatisfied dependencies: %s", ", ".join(sorted(res["unsatisfied"])))

    def download(
        self,
        packages: List[str],
        repo: Optional[List[str]] = None,
        downloaddir: Optional[str] = None,
        destdir: Optional[str] = None,
        resolve_flag: bool = False,
        recurse: Optional[int] = None,
        source: bool = False,
        urls: bool = False,
        arch: Optional[str] = None,
    ) -> None:

        repo_ids = self._resolve_repo_names_to_ids(repo) if repo else None
        targets: Dict[int, Dict[str, Any]] = {}

        if resolve_flag or recurse is not None:
            res = self._resolve_dependencies(
                packages,
                repo=repo,
                weakdeps=False,
                recursive=recurse,
                arch=arch,
            )

            if not res["roots"]:
                log.info("No packages matched the patterns or dependencies.")
                return

            for key in res["roots"]:
                targets[key] = res["packages"][key]

            for dep_entries in res["deplist"].values():
                for dep in dep_entries:
                    chosen = dep["chosen"]
                    targets[chosen["pkgKey"]] = chosen

        else:
            for pat in packages:
                rows = self.db.search_packages(pat, repo_filter=repo_ids, exact=True)
                if not rows:
                    log.warning("No match found for package: %s", pat)
                    continue
                best = max(rows, key=lambda r: NEVRA.from_row(r))
                targets[best["pkgKey"]] = best

        if not targets:
            log.info("No packages selected for download.")
            return

        target_rows = sorted(
            targets.values(),
            key=lambda r: NEVRA.from_row(r),
        )

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

            repo_row = self.db.get_repo(int(row["repo_id"])) if row.get("repo_id") else None
            if repo_row and lh:
                urls_list.append(f"{repo_row['base_url'].rstrip('/')}/{lh.lstrip('/')}")

            return urls_list

        if urls:
            for row in target_rows:
                nevra = NEVRA.from_row(row)
                ulist = build_urls_for_row(row)
                if not ulist:
                    log.info("%s -> no URL available", nevra)
                else:
                    for u in ulist:
                        print(u)
            return

        for row in target_rows:
            candidates: List[Dict[str, Any]] = [row]

            if source and row.get("rpm_sourcerpm"):
                src_rows = self.db.search_packages(
                    row["rpm_sourcerpm"],
                    repo_filter=None,
                    exact=True,
                )
                candidates.extend(src_rows)

            for pkg_row in candidates:
                nevra = NEVRA.from_row(pkg_row)
                urls_list = build_urls_for_row(pkg_row)

                if not urls_list:
                    log.warning("Skipping %s: no URL available", nevra)
                    continue

                url = urls_list[0]
                filename = url.split("/")[-1] or f"{nevra.to_nvra()}.rpm"
                outpath = download_dir / filename

                try:
                    if hasattr(self.downloader, "download_to_file"):
                        self.downloader.download_to_file(url, outpath)
                    else:
                        data = self.downloader.download_to_memory(url)
                        with open(outpath, "wb") as fh:
                            fh.write(data)

                    log.info("Downloaded %s -> %s", nevra, outpath)

                    if dest_dir:
                        final = dest_dir / filename
                        shutil.copy2(outpath, final)
                        log.info("Copied to %s", final)

                except Exception as e:
                    log.exception("Download failed for %s: %s", nevra, e)
