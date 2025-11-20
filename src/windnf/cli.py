# cli.py
import argparse
from . import operations


# -------------------------------------------------------------
# Helpers
# -------------------------------------------------------------
def parse_repoid(value):
    if not value:
        return None
    return [part.strip() for part in value.split(",") if part.strip()]


def main():
    parser = argparse.ArgumentParser(description="windnf (DNF simulator for Windows)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ---------------------------------------------------------
    # repoadd
    # ---------------------------------------------------------
    p = subparsers.add_parser("repoadd", help="Add a new repository")
    p.add_argument("name", help="Repository unique identifier")
    p.add_argument("baseurl", help="Base URL of the repository")
    p.add_argument(
        "--repomd",
        "-m",
        default="repodata/repomd.xml",
        help="Path to repository metadata XML (default: repodata/repomd.xml)",
    )
    p.set_defaults(func=operations.repoadd)

    # ---------------------------------------------------------
    # repolist
    # ---------------------------------------------------------
    p = subparsers.add_parser("repolist", help="List configured repositories")
    p.set_defaults(func=operations.repolist)

    # ---------------------------------------------------------
    # reposync
    # ---------------------------------------------------------
    p = subparsers.add_parser("reposync", help="Synchronize repository metadata")
    p.add_argument("names", nargs="*", help="Repository names to sync")
    p.add_argument("--all", "-A", action="store_true", help="Sync all repositories")
    p.set_defaults(func=operations.reposync)

    # ---------------------------------------------------------
    # repodel
    # ---------------------------------------------------------
    p = subparsers.add_parser("repodel", help="Delete repository and its packages")
    p.add_argument("names", nargs="*", help="Repository name(s) to delete")
    p.add_argument("--all", "-A", action="store_true", help="Delete all repositories")
    p.add_argument("--force", "-f", action="store_true", help="Force deletion")
    p.set_defaults(func=operations.repodel)

    # ---------------------------------------------------------
    # search
    # ---------------------------------------------------------
    p = subparsers.add_parser("search", help="Search packages")
    p.add_argument("patterns", nargs="+", help="Package search patterns (wildcards supported)")
    p.add_argument("--showduplicates", action="store_true", help="Show all package versions")
    p.add_argument(
        "--repo",
        "--repoid",
        "-r",
        dest="repoids",
        action="append",
        type=parse_repoid,
        help="Specify repositories (repeatable, comma-separated allowed)",
    )
    p.set_defaults(func=operations.search)

    # ---------------------------------------------------------
    # resolve
    # ---------------------------------------------------------
    p = subparsers.add_parser("resolve", help="Resolve dependencies (custom)")
    p.add_argument("packages", nargs="+", help="Packages to resolve (exact match)")
    p.add_argument(
        "--repo",
        "--repoid",
        "-r",
        dest="repoids",
        action="append",
        type=parse_repoid,
        help="Repositories to use while resolving dependencies",
    )
    p.add_argument("--weakdeps", "-w", action="store_true", help="Include weak/optional dependencies")
    p.add_argument("--recursive", "-R", action="store_true", help="Resolve dependencies recursively")
    p.add_argument("--arch", help="Target architecture (e.g., x86_64, aarch64)")
    p.set_defaults(func=operations.resolve)

    # ---------------------------------------------------------
    # download
    # ---------------------------------------------------------
    p = subparsers.add_parser("download", help="Download packages")
    p.add_argument("packages", nargs="+", help="Package specifiers (names, globs, provides, paths)")
    p.add_argument(
        "--repo",
        "--repoid",
        "-r",
        dest="repoids",
        action="append",
        type=parse_repoid,
        help="Repositories to use (repeatable, comma-separated allowed)",
    )
    p.add_argument("--downloaddir", "-x", help="Directory to save downloaded packages (default: CWD)")
    p.add_argument("--destdir", help="Alias for --downloaddir")
    p.add_argument("--resolve", action="store_true", help="Download dependencies too")
    p.add_argument("--recurse", "-R", action="store_true", help="Recursively download dependencies (implies --resolve)")
    p.add_argument("--source", "-S", action="store_true", help="Download SRPMs instead of binary RPMs")
    p.add_argument("--url", "--urls", dest="urls", action="store_true", help="Print URLs only (do not download)")
    p.add_argument("--arch", help="Architecture (e.g., x86_64, aarch64)")
    p.set_defaults(func=operations.download)

    # ---------------------------------------------------------
    # Parse arguments and normalize
    # ---------------------------------------------------------
    args = parser.parse_args()

    # Flatten repoids
    if getattr(args, "repoids", None):
        args.repoids = [repo for sublist in args.repoids for repo in sublist]

    # destdir alias
    if getattr(args, "destdir", None):
        args.downloaddir = args.destdir

    # Validate reposync & repodel
    if args.command == "reposync" and args.all and args.names:
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
        return args.func(None if args.all else args.names, args.all)
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
        if args.recurse:
            args.resolve = True
        return args.func(
            packages=args.packages,
            repoids=args.repoids,
            downloaddir=args.downloaddir,
            resolve=args.resolve,
            recurse=args.recurse,
            source=args.source,
            urls=args.urls,
            arch=args.arch,
        )
    else:
        parser.error("Internal error: unknown command.")


if __name__ == "__main__":
    main()
