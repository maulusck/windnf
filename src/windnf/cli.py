import argparse
import re
import shutil
import sys
from typing import List
from urllib.parse import urljoin

import urllib3

from .config import Config
from .db_manager import DbManager
from .downloader import Downloader
from .metadata_manager import MetadataManager
from .operations import (
    add_repo,
    delete_repo,
    download_packages,
    list_repos,
    resolve_dependencies_multiple,
    search_packages,
    sync_repos,
)
from .utils import Colors, _logger


def highlight_matches(text: str, patterns: List[str], color_code: str = Colors.YELLOW) -> str:
    combined_pattern = "|".join(re.escape(pat) for pat in patterns)
    regex = re.compile(combined_pattern, re.IGNORECASE)

    def replacer(match):
        return f"{color_code}{match.group(0)}{Colors.RESET}"

    return regex.sub(replacer, text)


class SplitCommaSeparated(argparse.Action):
    """Argparse action to split comma separated args into list"""

    def __call__(self, parser, namespace, values, option_string=None):
        items = [v.strip() for v in values.split(",") if v.strip()]
        setattr(namespace, self.dest, items)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="windnf - Windows DNF-like RPM package manager")
    subparsers = parser.add_subparsers(dest="command", required=True)

    repoadd = subparsers.add_parser("repoadd", help="Add/update a repository")
    repoadd.add_argument("name", help="Repository name")
    repoadd.add_argument("baseurl", help="Repository base URL")
    repoadd.add_argument(
        "--repomd",
        default="repodata/repomd.xml",
        help="Path to repomd.xml (default: repodata/repomd.xml)",
    )

    subparsers.add_parser("repolist", help="List all configured repositories")

    reposync = subparsers.add_parser("reposync", help="Sync repository metadata")
    reposync.add_argument(
        "repo",
        nargs="?",
        default="all",
        help="Repo name to sync or 'all' (default: all)",
    )

    repodel = subparsers.add_parser("repodel", help="Delete a repository and all its packages")
    repodel.add_argument("name", help="Repository name")

    search = subparsers.add_parser("search", help="Search packages")
    search.add_argument("pattern", nargs="+", help="Package name(s) or patterns (wildcards allowed)")
    search.add_argument(
        "--repo",
        action=SplitCommaSeparated,
        default=None,
        help="Comma separated repos to search (default: all)",
    )
    search.add_argument(
        "--showduplicates",
        action="store_true",
        help="Show all matching package versions (default: show only latest)",
    )

    resolve = subparsers.add_parser("resolve", help="Resolve package dependencies")
    resolve.add_argument("packages", nargs="+", help="Package name(s) to resolve")
    resolve.add_argument(
        "--repo",
        action=SplitCommaSeparated,
        default=None,
        help="Comma separated repository names",
    )
    resolve.add_argument("--recurse", action="store_true", help="Recursively resolve dependencies")
    resolve.add_argument("--weakdeps", action="store_true", help="Include weak dependencies")
    resolve.add_argument(
        "--showduplicates",
        action="store_true",
        help="Show all package versions in resolution (default: show only latest)",
    )

    download = subparsers.add_parser("download", help="Download packages")
    download.add_argument("packages", nargs="+", help="Package name(s) to download")
    download.add_argument(
        "--repo",
        action=SplitCommaSeparated,
        default=None,
        help="Comma separated repos to download from",
    )
    download.add_argument("--alldeps", action="store_true", help="Include dependencies in download")
    download.add_argument("--recurse", action="store_true", help="Recursively download dependencies")
    download.add_argument("--weakdeps", action="store_true", help="Include weak dependencies")
    download.add_argument(
        "--fetchduplicates",
        action="store_true",
        help="Download multiple versions of the same package (default: only latest)",
    )

    return parser


def print_packages(
    packages: List[dict],
    terminal_width: int,
    highlight_patterns: List[str] = None,
    title: str = None,
):
    if not packages:
        _logger.info("No matching packages found.")
        return

    if title:
        print(title)

    indent = "  "
    max_line_width = terminal_width - len(indent)

    name_col_width = 55
    repo_col_width = 30
    version_col_width = 35
    arch_col_width = 15

    for pkg in packages:
        pkg_name = pkg["name"]
        if highlight_patterns:
            pkg_name = highlight_matches(pkg_name, highlight_patterns)
        name_field = f"{Colors.BOLD}{Colors.CYAN}{pkg_name:<{name_col_width}}{Colors.RESET}"

        repo_field = f"{Colors.MAGENTA}From: {pkg['repo_name']:<{repo_col_width}}{Colors.RESET}"
        version_field = f"{Colors.GREEN}Ver: {pkg['version']}-{pkg['release']:<{version_col_width - 5}}{Colors.RESET}"
        arch_field = f"{Colors.YELLOW}Arch: {pkg['arch']:<{arch_col_width}}{Colors.RESET}"

        line = f"{name_field} {repo_field} {version_field} {arch_field}"

        plain_line = re.sub(r"\033\[[0-9;]*m", "", line)
        if len(plain_line) > max_line_width:
            line = line[: max_line_width - 3] + "..."

        print(indent + line)


def handle_command(args, config):
    db_manager = DbManager(config.db_path)
    downloader = Downloader(config.downloader, skip_ssl_verify=config.skip_ssl_verify)
    metadata_manager = MetadataManager(downloader, db_manager)

    terminal_width = shutil.get_terminal_size(fallback=(80, 20)).columns

    if args.command == "repoadd":
        add_repo(db_manager, args.name, args.baseurl, args.repomd)
        _logger.info(f"Repository '{args.name}' added or updated.")

    elif args.command == "repolist":
        list_repos(db_manager)

    elif args.command == "reposync":
        sync_repos(metadata_manager, db_manager, args.repo)

    elif args.command == "repodel":
        delete_repo(db_manager, args.name)
        _logger.info(f"Repository '{args.name}' deleted.")

    elif args.command == "search":
        pkgs = search_packages(
            db_manager,
            args.pattern,
            repo_names=args.repo,
            showduplicates=args.showduplicates,
        )
        print_packages(
            pkgs,
            terminal_width,
            highlight_patterns=args.pattern,
            title="Search results:",
        )

    elif args.command == "resolve":
        results = resolve_dependencies_multiple(
            db_manager,
            args.packages,
            repo_names=args.repo,
            recurse=args.recurse,
            include_weak=args.weakdeps,
        )

        if not results:
            _logger.info("No dependencies found.")
            return

        for root_pkg, deps in results.items():
            if not deps:
                _logger.info(f"No dependencies found for package '{root_pkg}'.")
                continue

            print(f"Dependencies for package: {root_pkg}")

            dep_infos = search_packages(
                db_manager,
                list(deps),
                repo_names=args.repo,
                showduplicates=False,
                exact_match=True,
            )

            print_packages(dep_infos, terminal_width)

    elif args.command == "download":
        download_packages(
            db_manager,
            metadata_manager,
            config,
            args.packages,
            repo_names=args.repo,
            download_deps=args.alldeps,
            recurse=args.recurse,
            include_weak=args.weakdeps,
            fetchduplicates=args.fetchduplicates,
        )

    else:
        _logger.error(f"Unknown command: {args.command}")


def main():
    try:
        parser = create_parser()
        args = parser.parse_args()
        config = Config()

        if config.skip_ssl_verify:
            _logger.warning("SSL verification disabled; HTTPS requests insecure.")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        handle_command(args, config)

    except KeyboardInterrupt:
        _logger.warning("Terminated by user (Ctrl+C). Exiting...")
        sys.exit(130)

    except Exception as e:
        _logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
