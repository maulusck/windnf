import argparse
import sys
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
    resolve_dependencies,
    search_packages,
    sync_repos,
)
from .utils import _logger


class SplitCommaSeparated(argparse.Action):
    """Argparse action to split comma separated args into list"""

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, [v.strip() for v in values.split(",") if v.strip()])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="windnf - Windows DNF-like RPM package manager")
    subparsers = parser.add_subparsers(dest="command", required=True)

    repoadd_parser = subparsers.add_parser("repoadd", help="Add/update a repository")
    repoadd_parser.add_argument("name", help="Repository name")
    repoadd_parser.add_argument("baseurl", help="Repository base URL")
    repoadd_parser.add_argument(
        "--repomd",
        default="repodata/repomd.xml",
        help="Path to repomd.xml (default: repodata/repomd.xml)",
    )

    subparsers.add_parser("repolist", help="List all configured repositories")

    sync_parser = subparsers.add_parser("reposync", help="Sync repository metadata")
    sync_parser.add_argument(
        "repo",
        nargs="?",
        default="all",
        help="Repo name to sync or 'all' (default: all)",
    )

    repodel_parser = subparsers.add_parser("repodel", help="Delete a repository and all its packages")
    repodel_parser.add_argument("name", help="Repository name")

    search_parser = subparsers.add_parser("search", help="Search packages")
    search_parser.add_argument("pattern", help="Package name or pattern (wildcards allowed)")
    search_parser.add_argument(
        "--repo",
        help="Comma separated repos to search (default: all)",
        action=SplitCommaSeparated,
        default=None,
    )
    search_parser.add_argument(
        "--showduplicates",
        action="store_true",
        help="Show all matching package versions (default: show only latest)",
    )

    resolve_parser = subparsers.add_parser("resolve", help="Resolve package dependencies")
    resolve_parser.add_argument("packages", nargs="+", help="Package name(s) to resolve")
    resolve_parser.add_argument(
        "--repo",
        help="Comma separated repository names",
        action=SplitCommaSeparated,
        default=None,
    )
    resolve_parser.add_argument("--recurse", action="store_true", help="Recursively resolve dependencies")
    resolve_parser.add_argument("--weakdeps", action="store_true", help="Include weak dependencies")
    resolve_parser.add_argument(
        "--showduplicates",
        action="store_true",
        help="Show all package versions in resolution (default: show only latest)",
    )

    download_parser = subparsers.add_parser("download", help="Download packages")
    download_parser.add_argument("packages", nargs="+", help="Package name(s) to download")
    download_parser.add_argument(
        "--repo",
        help="Comma separated repos to download from",
        action=SplitCommaSeparated,
        default=None,
    )
    download_parser.add_argument("--alldeps", action="store_true", help="Include dependencies in download")
    download_parser.add_argument("--recurse", action="store_true", help="Recursively download dependencies")
    download_parser.add_argument("--weakdeps", action="store_true", help="Include weak dependencies")
    download_parser.add_argument(
        "--fetchduplicates",
        action="store_true",
        help="Download multiple versions of the same package (default: only latest)",
    )

    return parser.parse_args()


def main() -> None:
    try:
        args = parse_args()
        config = Config()

        if config.skip_ssl_verify:
            _logger.warning("SSL verification disabled; HTTPS requests insecure.")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        db_manager = DbManager(config.db_path)
        downloader = Downloader(config.downloader, skip_ssl_verify=config.skip_ssl_verify)
        metadata_manager = MetadataManager(downloader, db_manager)

        cmd = args.command

        if cmd == "repoadd":
            add_repo(db_manager, args.name, args.baseurl, args.repomd)
            _logger.info(f"Repository '{args.name}' added or updated.")
        elif cmd == "repolist":
            list_repos(db_manager)
        elif cmd == "reposync":
            sync_repos(metadata_manager, db_manager, args.repo)
        elif cmd == "repodel":
            delete_repo(db_manager, args.name)
            _logger.info(f"Repository '{args.name}' deleted.")
        elif cmd == "search":
            search_packages(db_manager, args.pattern, repo_names=args.repo, showduplicates=args.showduplicates)
        elif cmd == "resolve":
            for pkg in args.packages:
                resolve_dependencies(
                    db_manager,
                    pkg,
                    repo_names=args.repo,
                    recurse=args.recurse,
                    include_weak=args.weakdeps,
                    showduplicates=args.showduplicates,
                )
        elif cmd == "download":
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
            _logger.error(f"Unknown command: {cmd}")

    except KeyboardInterrupt:
        _logger.warning("Terminated by user (Ctrl+C). Exiting...")
        sys.exit(130)

    except Exception as e:
        _logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
