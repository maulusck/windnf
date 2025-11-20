# tests/run.py
import io
import os
import sys
from contextlib import redirect_stdout
from pathlib import Path

from windnf import cli

# -----------------------------------------------------------
# Setup test directories
# -----------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()  # directory of this script (tests/)
os.chdir(SCRIPT_DIR)  # change working directory to testdir
DOWNLOAD_DIR = SCRIPT_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

# Repository info
REPO1_NAME = "epel9"
REPO1_BASEURL = "https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/"
REPOMD1_URL = f"{REPO1_BASEURL}repodata/repomd.xml"

REPO2_NAME = "zabbix9"
REPO2_BASEURL = "https://repo.zabbix.com/zabbix/7.0/centos/9/x86_64/"
REPOMD2_URL = f"{REPO2_BASEURL}repodata/repomd.xml"


# -----------------------------------------------------------
# Helper to run CLI commands
# -----------------------------------------------------------
def run(*args):
    """Run a CLI command and capture stdout."""
    print(f"\033[36m[CMD]\033[0m {' '.join(args)}")
    f = io.StringIO()
    sys_argv_backup = sys.argv
    try:
        sys.argv = ["windnf"] + list(args)
        with redirect_stdout(f):
            cli.main()
    finally:
        sys.argv = sys_argv_backup
    output = f.getvalue()
    if output:
        print(output)
    return output


# -----------------------------------------------------------
# Test suite
# -----------------------------------------------------------
def main():
    print(f"Starting windnf test suite in {SCRIPT_DIR}...\n")

    # ====================================================
    # REPOADD
    # ====================================================
    run("repoadd", REPO1_NAME, REPO1_BASEURL, "--repomd", REPOMD1_URL)
    run("repoadd", REPO2_NAME, REPO2_BASEURL, "--repomd", REPOMD2_URL)

    # ====================================================
    # REPOLIST
    # ====================================================
    run("repolist")

    # ====================================================
    # REPOSYNC
    # ====================================================
    run("reposync", "notarepo")
    run("reposync", "--all")

    # ====================================================
    # SEARCH
    # ====================================================
    run("search", "bash")
    run("search", "*ash")
    run("search", "bash*")
    run("search", "*bash*")
    run("search", "bash", "--showduplicates")
    run("search", "bash", "--repo", REPO1_NAME)
    run("search", "bash", "--repo", REPO2_NAME)
    run("search", "bash", "--repo", "notarepo")

    # ====================================================
    # RESOLVE
    # ====================================================
    run("resolve", "vlc")
    run("resolve", "vlc", "--recursive")
    run("resolve", "vlc", "--weakdeps")
    run("resolve", "vlc", "--arch", "x86_64")
    run("resolve", "vlc", "--arch", "arm64")
    run("resolve", "vlc", "--repo", REPO1_NAME)
    run("resolve", "vlc", "--repo", "notarepo")

    # ====================================================
    # DOWNLOAD
    # ====================================================
    # Use DOWNLOAD_DIR for all downloads
    run("download", "vlc", "--urls")
    run("download", "vlc-plugin*", "--urls")
    run("download", "vlc", "--downloaddir", str(DOWNLOAD_DIR), "--urls")
    run("download", "vlc", "--resolve", "--urls", "--downloaddir", str(DOWNLOAD_DIR))
    run("download", "vlc", "--source", "--urls", "--downloaddir", str(DOWNLOAD_DIR))
    run("download", "vlc", "--arch", "x86_64", "--urls", "--downloaddir", str(DOWNLOAD_DIR))
    run("download", "vlc", "--repo", REPO1_NAME, "--urls", "--downloaddir", str(DOWNLOAD_DIR))

    # ====================================================
    # REPODEL
    # ====================================================
    # run("repodel", REPO1_NAME, "--force")
    # run("repodel", REPO2_NAME, "--force")
    # run("repolist")

    # # Re-add for --all deletion
    # run("repoadd", REPO1_NAME, REPO1_BASEURL, "--repomd", REPOMD1_URL)
    # run("repoadd", REPO2_NAME, REPO2_BASEURL, "--repomd", REPOMD2_URL)
    # run("repodel", "--all", "--force")
    # run("repolist")

    print("\033[32mAll tests completed successfully.\033[0m")


if __name__ == "__main__":
    main()
