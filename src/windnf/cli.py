# cli.py
import argparse
import sys

from . import operations
from .config import Config


def main():
    # ------------------------
    # Initialize config + operations
    # ------------------------
    config = Config()
    operations.init(config)

    parser = argparse.ArgumentParser(prog="windnf", description="WINDNF package manager CLI")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # ------------------------
    # Repository Commands
    # ------------------------

    # repoadd / ra
    p_repoadd = subparsers.add_parser("repoadd", aliases=["ra"], help="Add (or update) a repository")
    p_repoadd.add_argument("name")
    p_repoadd.add_argument("baseurl")
    p_repoadd.add_argument("--repomd", "-m", default="repodata/repomd.xml")
    p_repoadd.add_argument("--type", "-t", dest="repo_type", choices=["binary", "source"], default="binary")
    p_repoadd.add_argument("--source-repo", "-s")
    p_repoadd.set_defaults(func=operations.repoadd)

    # repolink / rlk
    p_repolink = subparsers.add_parser("repolink", aliases=["rlk"], help="Link source repo → binary repo")
    p_repolink.add_argument("binary_repo")
    p_repolink.add_argument("source_repo")
    p_repolink.set_defaults(func=operations.repolink)

    # repolist / rl
    p_repolist = subparsers.add_parser("repolist", aliases=["rl"], help="List repositories")
    p_repolist.set_defaults(func=operations.repolist)

    # reposync / rs
    p_reposync = subparsers.add_parser("reposync", aliases=["rs"], help="Sync repository metadata")
    p_reposync.add_argument("names", nargs="*", help="Repository names")
    p_reposync.add_argument("--all", "-A", dest="all_", action="store_true")
    p_reposync.set_defaults(func=operations.reposync)

    # repodel / rd
    p_repodel = subparsers.add_parser("repodel", aliases=["rd"], help="Delete repositories")
    p_repodel.add_argument("names", nargs="*", help="Repository names")
    p_repodel.add_argument("--all", "-A", dest="all_", action="store_true")
    p_repodel.add_argument("--force", "-f", action="store_true")
    p_repodel.set_defaults(func=operations.repodel)

    # ------------------------
    # Package Queries
    # ------------------------

    # search / s
    p_search = subparsers.add_parser("search", aliases=["s"], help="Search for packages")
    p_search.add_argument("patterns", nargs="+")
    p_search.add_argument("--repo", "--repoid", "-r", nargs="*", help="Repository names")
    p_search.add_argument("--showduplicates", action="store_true")
    p_search.set_defaults(func=operations.search)

    # info / i
    p_info = subparsers.add_parser("info", aliases=["i"], help="Show full NEVRA package information")
    p_info.add_argument("pattern")
    p_info.add_argument("--repo", "--repoid", "-r", nargs="*", help="Repository names")
    p_info.set_defaults(func=operations.info)

    # ------------------------
    # Dependency Resolution
    # ------------------------

    # resolve / rv
    p_resolve = subparsers.add_parser("resolve", aliases=["rv"], help="Resolve dependency sets")
    p_resolve.add_argument("packages", nargs="+")
    p_resolve.add_argument("--repo", "--repoid", "-r", nargs="*", help="Repository names")
    p_resolve.add_argument("--weakdeps", "-w", action="store_true")
    p_resolve.add_argument("--recursive", "-R", action="store_true")
    p_resolve.add_argument("--arch")
    p_resolve.set_defaults(func=operations.resolve)

    # ------------------------
    # Download / dl
    # ------------------------

    p_download = subparsers.add_parser("download", aliases=["dl"], help="Download packages / SRPMs")
    p_download.add_argument("packages", nargs="+")
    p_download.add_argument("--repo", "--repoid", "-r", nargs="*", help="Repository names")
    p_download.add_argument("--downloaddir", "-x", type=str)
    p_download.add_argument("--destdir", type=str)
    p_download.add_argument("--resolve", action="store_true")
    p_download.add_argument("--recurse", "-R", action="store_true")
    p_download.add_argument("--source", "-S", action="store_true")
    p_download.add_argument("--urls", "--url", action="store_true")
    p_download.add_argument("--arch")
    p_download.set_defaults(func=operations.download)

    # ------------------------
    # Parse and execute
    # ------------------------
    args = parser.parse_args()
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        sys.exit(1)

    arg_dict = vars(args)
    arg_dict.pop("func", None)
    arg_dict.pop("command", None)

    func(**arg_dict)


if __name__ == "__main__":
    main()
