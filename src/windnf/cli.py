import argparse

from . import operations



def main():
    parser = argparse.ArgumentParser(description="windnf CLI tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_repoadd = subparsers.add_parser("repoadd", help="Add a new repository")
    p_repoadd.add_argument("name", help="Repository unique identifier")
    p_repoadd.add_argument("baseurl", help="Base URL of repository")
    p_repoadd.add_argument("--repomd", "-m", default="repodata/repomd.xml", help="Repository metadata XML path")
    p_repoadd.set_defaults(func=lambda args: operations.repoadd(args.name, args.baseurl, args.repomd))

    p_repolist = subparsers.add_parser("repolist", help="List configured repositories")
    p_repolist.set_defaults(func=lambda args: operations.repolist())

    p_reposync = subparsers.add_parser("reposync", help="Synchronize repository metadata")
    p_reposync.add_argument("name", nargs="*", help="Repository name(s) to sync")
    p_reposync.add_argument("--all", "-A", action="store_true", help="Sync all repositories")
    p_reposync.set_defaults(func=lambda args: operations.reposync(args.name if args.name else None, args.all))

    p_repodel = subparsers.add_parser("repodel", help="Delete repository and associated packages")
    p_repodel.add_argument("name", nargs="?", help="Name of repository to delete")
    p_repodel.add_argument("--force", "-f", action="store_true", help="Force delete without confirmation")
    p_repodel.add_argument("--all", "-A", action="store_true", help="Delete all repositories and packages")
    p_repodel.set_defaults(func=lambda args: operations.repodel(args.name, args.force, args.all))

    p_search = subparsers.add_parser("search", help="Search for packages")
    p_search.add_argument("patterns", nargs="+", help="Package names or package:version patterns to search")
    p_search.add_argument("--repo", "-r", help="Comma-separated list of repositories to search")
    p_search.add_argument("--showduplicates", "-d", action="store_true", help="Show duplicate package versions")
    p_search.set_defaults(func=lambda args: operations.search(args.patterns, args.repo, args.showduplicates))

    p_resolve = subparsers.add_parser("resolve", help="Resolve dependencies for packages")
    p_resolve.add_argument("packages", nargs="+", help="Packages or package:version specs to resolve")
    p_resolve.add_argument("--repo", "-r", help="Comma-separated list of repositories to resolve from")
    p_resolve.add_argument("--recurse", "-R", action="store_true", help="Recursively resolve dependencies")
    p_resolve.add_argument("--weakdeps", "-w", action="store_true", help="Include weak/optional dependencies")
    p_resolve.set_defaults(func=lambda args: operations.resolve(args.packages, args.repo, args.recurse, args.weakdeps))

    p_download = subparsers.add_parser("download", help="Download packages")
    p_download.add_argument("packages", nargs="+", help="Packages or package:version to download")
    p_download.add_argument("--repo", "-r", help="Comma-separated list of repositories to download from")
    p_download.add_argument("--alldeps", "-a", action="store_true", help="Download all dependencies")
    p_download.add_argument("--recurse", "-R", action="store_true", help="Recursively download dependencies")
    p_download.add_argument("--weakdeps", "-w", action="store_true", help="Include weak dependencies")
    p_download.add_argument("--fetchduplicates", "-f", action="store_true", help="Download duplicate versions")
    p_download.add_argument("--url", "-u", action="store_true", help="Print URLs instead of downloading")
    p_download.set_defaults(
        func=lambda args: operations.download(
            args.packages, args.repo, args.alldeps, args.recurse, args.weakdeps, args.fetchduplicates, args.url
        )
    )

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
