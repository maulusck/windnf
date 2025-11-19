# test_cli.py
import io
import os
import sys
from contextlib import redirect_stdout

from windnf import cli

# -----------------------------------------------------------
# Test configuration
# -----------------------------------------------------------
TESTDIR = "tests"
DOWNLOAD_DIR = os.path.join(TESTDIR, "downloads")

REPO1_NAME = "epel9"
REPO1_BASEURL = "https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/"
REPOMD1_URL = f"{REPO1_BASEURL}repodata/repomd.xml"

REPO2_NAME = "zabbix9"
REPO2_BASEURL = "https://repo.zabbix.com/zabbix/7.0/centos/9/x86_64/"
REPOMD2_URL = f"{REPO2_BASEURL}repodata/repomd.xml"

# Create test directories
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


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
    print("Starting windnf test suite...\n")

    # ====================================================
    # REPOADD
    # ====================================================
    run("repoadd", REPO1_NAME, REPO1_BASEURL)
    run("repoadd", REPO1_NAME, REPO1_BASEURL, "--repomd", REPOMD1_URL)
    run("repoadd", REPO2_NAME, REPO2_BASEURL, "--repomd", REPOMD2_URL)

    # ====================================================
    # REPOLIST
    # ====================================================
    run("repolist")

    # ====================================================
    # REPOSYNC
    # ====================================================
    # run("reposync", REPO1_NAME)
    # run("reposync", REPO1_NAME, REPO2_NAME)
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

    print("Running zabbix-agent2 search tests...")
    run("search", "zabbix-agent2", "--repo", REPO1_NAME)
    run("search", "zabbix-agent2", "--repo", REPO2_NAME)
    run("search", "zabbix-agent2", "--repo", REPO1_NAME, "--repo", REPO2_NAME)
    run("search", "zabbix-agent2", "--repo", f"{REPO1_NAME},{REPO2_NAME}")
    run("search", "zabbix-agent2", "--repo", "notarepo")
    run("search", "zabbix-agent2")  # default all repos

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
    run("download", "bash", "--urls")
    run("download", "*ash", "--urls")
    run("download", "bash", "--downloaddir", DOWNLOAD_DIR, "--urls")
    run("download", "bash", "--resolve", "--urls")
    run("download", "bash", "--source", "--urls")
    run("download", "bash", "--arch", "x86_64", "--urls")
    run("download", "bash", "--repo", REPO1_NAME, "--urls")

    # ====================================================
    # REPODEL
    # ====================================================
    run("repodel", REPO1_NAME, "--force")
    run("repodel", REPO2_NAME, "--force")
    run("repolist")

    # Re-add for --all deletion
    run("repoadd", REPO1_NAME, REPO1_BASEURL, "--repomd", REPOMD1_URL)
    run("repoadd", REPO2_NAME, REPO2_BASEURL, "--repomd", REPOMD2_URL)

    run("repodel", "--all", "--force")
    run("repolist")

    print("\033[32mAll tests completed successfully.\033[0m")


if __name__ == "__main__":
    main()
