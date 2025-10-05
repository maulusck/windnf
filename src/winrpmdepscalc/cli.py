import argparse
import sys
from pathlib import Path
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="windnf - Windows DNF-like RPM package manager")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Repo add (renamed from add)
    repoadd_parser = subparsers.add_parser("repoadd", help="Add a repository")
    repoadd_parser.add_argument("name", help="Repository name")
    repoadd_parser.add_argument("baseurl", help="Repository base URL")
    repoadd_parser.add_argument(
        "--repomd",
        default="repodata/repomd.xml",
        help="Repomd.xml relative path (default: repodata/repomd.xml)",
    )

    # List repos
    subparsers.add_parser("repolist", help="List all configured repositories")

    # Sync repos
    sync_parser = subparsers.add_parser("reposync", help="Sync repository metadata")
    sync_parser.add_argument(
        "repo",
        nargs="?",
        default="all",
        help="Repository name to sync or 'all' to sync all repositories",
    )

    # Delete repo
    repodel_parser = subparsers.add_parser("repodel", help="Delete a repository and all its packages")
    repodel_parser.add_argument("name", help="Repository name to delete")

    # Search packages
    search_parser = subparsers.add_parser("search", help="Search packages")
    search_parser.add_argument("pattern", help="Package name or pattern (wildcards allowed)")
    search_parser.add_argument(
        "--repos", help="Comma separated list of repositories to search (default: all)", default=None
    )

    # Resolve dependencies (multiple packages)
    resolve_parser = subparsers.add_parser("resolve", help="Resolve package dependencies")
    resolve_parser.add_argument("packages", nargs="+", help="Package name(s) to resolve")
    resolve_parser.add_argument("--repo", help="Repository name")
    resolve_parser.add_argument("--recurse", action="store_true", help="Recursively resolve dependencies")
    resolve_parser.add_argument("--weakdeps", action="store_true", help="Include weak dependencies")

    # Download packages
    download_parser = subparsers.add_parser("download", help="Download packages")
    download_parser.add_argument("packages", nargs="+", help="Package name(s) to download")
    download_parser.add_argument("--repo", help="Comma separated list of repositories to download from")
    download_parser.add_argument("--alldeps", action="store_true", help="Download dependencies also")
    download_parser.add_argument("--recurse", action="store_true", help="Recursively download dependencies")
    download_parser.add_argument("--weakdeps", action="store_true", help="Include weak dependencies")

    return parser.parse_args()


def main() -> None:
    try:
        args = parse_args()
        config = Config()  # Loads ~/.windnf.conf or creates default config

        if config.skip_ssl_verify:
            _logger.warning("SSL verification disabled; HTTPS requests insecure.")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        db_manager = DbManager(config.db_path)
        downloader = Downloader(config.downloader, skip_ssl_verify=config.skip_ssl_verify)
        metadata_manager = MetadataManager(downloader, db_manager)

        cmd = args.command

        if cmd == "repoadd":
            add_repo(db_manager, args.name, args.baseurl, args.repomd)
        elif cmd == "repolist":
            list_repos(db_manager)
        elif cmd == "reposync":
            sync_repos(metadata_manager, db_manager, args.repo)
        elif cmd == "repodel":
            delete_repo(db_manager, args.name)
        elif cmd == "search":
            repos = None
            if args.repos:
                repos = [r.strip() for r in args.repos.split(",") if r.strip()]
            search_packages(db_manager, args.pattern, repos)
        elif cmd == "resolve":
            for pkg in args.packages:
                resolve_dependencies(
                    db_manager,
                    pkg,
                    repo_name=args.repo,
                    recurse=args.recurse,
                    include_weak=args.weakdeps,
                )
        elif cmd == "download":
            repo_names = None
            if args.repo:
                repo_names = [r.strip() for r in args.repo.split(",") if r.strip()]
            download_packages(
                db_manager,
                metadata_manager,
                config,
                args.packages,
                repo_names=repo_names,
                download_deps=args.alldeps,
                recurse=args.recurse,
                include_weak=args.weakdeps,
            )

        else:
            _logger.error(f"Unknown command: {cmd}")

    except KeyboardInterrupt:
        _logger.warning("Terminated by user (Ctrl+C). Exiting...")
        sys.exit(0)

    except Exception as e:
        _logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
