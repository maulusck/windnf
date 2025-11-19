# cli.py
import argparse

from . import operations


# -------------------------------------------------------------
# Helper: parse comma-separated repository IDs
# -------------------------------------------------------------
def parse_repoid(value):
    if not value:
        return None
    repos = []
    for part in value.split(","):
        part = part.strip()
        if part:
            repos.append(part)
    return repos


def main():
    parser = argparse.ArgumentParser(description="windnf CLI tool (DNF-style commands)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ===============================================================
    # repoadd
    # ===============================================================
    p_repoadd = subparsers.add_parser("repoadd", help="Add a new repository")
    p_repoadd.add_argument("name", help="Repository unique identifier")
    p_repoadd.add_argument("baseurl", help="Base URL of the repository")
    p_repoadd.add_argument(
        "--repomd",
        "-m",
        default="repodata/repomd.xml",
        help="Path to repository metadata XML (default: repodata/repomd.xml)",
    )
    p_repoadd.set_defaults(func=operations.repoadd)

    # ===============================================================
    # repolist
    # ===============================================================
    p_repolist = subparsers.add_parser("repolist", help="List configured repositories")
    p_repolist.set_defaults(func=operations.repolist)

    # ===============================================================
    # reposync
    # ===============================================================
    p_reposync = subparsers.add_parser("reposync", help="Synchronize repository metadata")
    p_reposync.add_argument("names", nargs="*", help="Repository names to sync")
    p_reposync.add_argument("--all", "-A", action="store_true", help="Sync all repositories")
    p_reposync.set_defaults(func=operations.reposync)

    # ===============================================================
    # repodel
    # ===============================================================
    p_repodel = subparsers.add_parser("repodel", help="Delete repository and its packages")
    p_repodel.add_argument("names", nargs="*", help="Repository name(s) to delete")
    p_repodel.add_argument("--all", "-A", action="store_true", help="Delete all repositories")
    p_repodel.add_argument("--force", "-f", action="store_true", help="Force deletion")
    p_repodel.set_defaults(func=operations.repodel)

    # ===============================================================
    # search
    # ===============================================================
    p_search = subparsers.add_parser("search", help="Search packages")
    p_search.add_argument("patterns", nargs="+", help="Package search patterns (wildcards supported)")
    p_search.add_argument("--showduplicates", action="store_true", help="Show all package versions")
    p_search.add_argument(
        "--repo",
        "--repoid",
        "-r",
        dest="repoids",
        action="append",
        type=parse_repoid,
        help="Specify repositories (repeatable, comma-separated allowed)",
    )
    p_search.set_defaults(func=operations.search)

    # ===============================================================
    # resolve
    # ===============================================================
    p_resolve = subparsers.add_parser("resolve", help="Resolve dependencies (custom)")
    p_resolve.add_argument("packages", nargs="+", help="Packages to resolve (exact match)")
    p_resolve.add_argument(
        "--repo",
        "--repoid",
        "-r",
        dest="repoids",
        action="append",
        type=parse_repoid,
        help="Repositories to use while resolving dependencies",
    )
    p_resolve.add_argument("--weakdeps", "-w", action="store_true", help="Include weak/optional dependencies")
    p_resolve.add_argument("--recursive", "-R", action="store_true", help="Resolve dependencies recursively")
    p_resolve.add_argument("--arch", help="Target architecture (e.g., x86_64, aarch64)")
    p_resolve.set_defaults(func=operations.resolve)

    # ===============================================================
    # download
    # ===============================================================
    p_download = subparsers.add_parser("download", help="Download packages")
    p_download.add_argument("packages", nargs="+", help="Package specifiers (names, globs, provides, paths)")
    p_download.add_argument(
        "--repo",
        "--repoid",
        "-r",
        dest="repoids",
        action="append",
        type=parse_repoid,
        help="Repositories to use (repeatable, comma-separated allowed)",
    )
    p_download.add_argument("--downloaddir", "-x", help="Directory to save downloaded packages (default: CWD)")
    p_download.add_argument("--destdir", help="Alias for --downloaddir")
    p_download.add_argument("--resolve", action="store_true", help="Download all dependencies too")
    p_download.add_argument("--source", action="store_true", help="Download SRPMs instead of binary RPMs")
    p_download.add_argument(
        "--urls", "--urlsonly", dest="urls", action="store_true", help="Print URLs only (do not download)"
    )
    p_download.add_argument("--arch", help="Architecture (e.g., x86_64, aarch64)")
    p_download.set_defaults(func=operations.download)

    # ===============================================================
    # Parse args
    # ===============================================================
    args = parser.parse_args()

    # Flatten repoids from nested lists into one list
    if hasattr(args, "repoids") and args.repoids:
        merged = []
        for lst in args.repoids:
            merged.extend(lst)
        args.repoids = merged
    else:
        args.repoids = None

    # Handle destdir alias
    if getattr(args, "destdir", None):
        args.downloaddir = args.destdir

    # Validate reposync & repodel
    if args.command == "reposync":
        if args.all and args.names:
            parser.error("reposync: cannot specify names together with --all.")

    if args.command == "repodel":
        if args.names and args.all:
            parser.error("repodel: cannot specify names together with --all.")
        if not args.names and not args.all:
            parser.error("repodel: specify one or more repository names or use --all.")

    # ===============================================================
    # Dispatch
    # ===============================================================
    if args.command == "repoadd":
        return args.func(args.name, args.baseurl, args.repomd)

    elif args.command == "repolist":
        return args.func()

    elif args.command == "reposync":
        repo_names = None if args.all else args.names
        return args.func(repo_names, args.all)

    elif args.command == "repodel":
        return args.func(args.names, args.force, args.all)

    elif args.command == "search":
        return args.func(patterns=args.patterns, repoids=args.repoids, showduplicates=args.showduplicates)

    elif args.command == "resolve":
        return args.func(
            packages=args.packages,
            repoids=args.repoids,
            recursive=args.recursive,
            weakdeps=args.weakdeps,
            arch=args.arch,
        )

    elif args.command == "download":
        return args.func(
            packages=args.packages,
            repoids=args.repoids,
            downloaddir=args.downloaddir,
            resolve=args.resolve,
            source=args.source,
            urls=args.urls,
            arch=args.arch,
        )

    else:
        parser.error("Internal error: unknown command.")


if __name__ == "__main__":
    main()
