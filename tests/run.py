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
SCRIPT_DIR = Path(__file__).parent.resolve()
os.chdir(SCRIPT_DIR)

DOWNLOAD_DIR = SCRIPT_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

# -----------------------------------------------------------
# Repository definitions
# -----------------------------------------------------------
REPO1_NAME = "epel9"
REPO1_BASEURL = "https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/"
REPOMD1_URL = f"{REPO1_BASEURL}repodata/repomd.xml"

REPO1_SRC_NAME = "epel9-source"
REPO1_SRC_BASEURL = "https://dl.fedoraproject.org/pub/epel/9/Everything/source/tree/"
REPOMD1_SRC_URL = f"{REPO1_SRC_BASEURL}repodata/repomd.xml"

REPO2_NAME = "zabbix9"
REPO2_BASEURL = "https://repo.zabbix.com/zabbix/7.0/centos/9/x86_64/"
REPOMD2_URL = f"{REPO2_BASEURL}repodata/repomd.xml"


# -----------------------------------------------------------
# Helper to run CLI commands
# -----------------------------------------------------------
def run(*args):
    """Run CLI command and capture stdout."""
    print(f"\033[36m[CMD]\033[0m {' '.join(args)}")

    buf = io.StringIO()
    original_argv = sys.argv
    try:
        sys.argv = ["windnf"] + list(args)
        with redirect_stdout(buf):
            cli.main()
    finally:
        sys.argv = original_argv

    out = buf.getvalue()
    if out:
        print(out)
    return out


# -----------------------------------------------------------
# Main test execution
# -----------------------------------------------------------
def main():
    print(f"Starting windnf test suite in {SCRIPT_DIR}...\n")

    # ====================================================
    # REPOADD — binary + source repos
    # ====================================================
    run("ra", REPO1_NAME, REPO1_BASEURL, "-m", REPOMD1_URL)
    run("ra", REPO1_SRC_NAME, REPO1_SRC_BASEURL, "-t", "source", "-m", REPOMD1_SRC_URL)

    # Auto-link source repo at add time
    run("ra", "linked-epel9", REPO1_BASEURL, "-m", REPOMD1_URL, "-s", REPO1_SRC_NAME)

    # Zabbix binary repo
    run("ra", REPO2_NAME, REPO2_BASEURL, "-m", REPOMD2_URL)

    # ====================================================
    # REPOLINK — explicit linking
    # ====================================================
    run("rlk", REPO1_NAME, REPO1_SRC_NAME)
    run("rlk", "notarepo", REPO1_SRC_NAME)  # invalid
    run("rlk", REPO1_NAME, "notasource")  # invalid

    # ====================================================
    # REPOLIST
    # ====================================================
    run("rl")

    # ====================================================
    # REPOSYNC
    # ====================================================
    run("rs", REPO1_NAME)
    run("rs", REPO1_SRC_NAME)
    run("rs", REPO2_NAME)
    run("rs", "notarepo")  # invalid
    run("rs", "-A")  # sync all

    # ====================================================
    # SEARCH — basic & repo-filtered
    # ====================================================
    patterns = ["bash", "*ash", "bash*", "*bash*"]
    for p in patterns:
        run("s", p)

    run("s", "bash", "--showduplicates")
    run("s", "bash", "-r", REPO1_NAME)
    run("s", "bash", "-r", REPO1_SRC_NAME)
    run("s", "bash", "-r", REPO2_NAME)
    run("s", "bash", "-r", "notarepo")  # invalid

    # ====================================================
    # INFO — package details
    # ====================================================
    run("i", "bash")
    run("i", "bash", "-r", REPO1_NAME)
    run("i", "bash", "-r", REPO1_SRC_NAME)
    run("i", "bash", "-r", "notarepo")  # invalid

    # ====================================================
    # RESOLVE — dependencies
    # ====================================================
    run("rv", "vlc")
    run("rv", "vlc", "-R")  # recursive
    run("rv", "vlc", "-w")  # weak dependencies
    run("rv", "vlc", "--arch", "x86_64")
    run("rv", "vlc", "--arch", "arm64")
    run("rv", "vlc", "-r", REPO1_NAME)
    run("rv", "vlc", "-r", REPO1_SRC_NAME)
    run("rv", "vlc", "-r", "notarepo")  # invalid

    # ====================================================
    # DOWNLOAD — binaries, SRPMs, dependencies
    # ====================================================
    run("dl", "vlc", "--urls")
    run("dl", "vlc-plugin*", "--urls")
    run("dl", "vlc", "-x", str(DOWNLOAD_DIR), "--urls")
    run("dl", "vlc", "--resolve", "-x", str(DOWNLOAD_DIR), "--urls")
    run("dl", "vlc", "-S", "-x", str(DOWNLOAD_DIR), "--urls")
    run("dl", "bash", "-S", "-x", str(DOWNLOAD_DIR), "--urls")
    run("dl", "vlc", "--arch", "x86_64", "-x", str(DOWNLOAD_DIR), "--urls")
    run("dl", "vlc", "-r", REPO1_NAME, "-x", str(DOWNLOAD_DIR), "--urls")
    run("dl", "vlc", "-r", REPO1_SRC_NAME, "-S", "-x", str(DOWNLOAD_DIR), "--urls")

    # ====================================================
    # REPODEL — remove repos
    # ====================================================
    run("rd", REPO1_NAME, "-f")
    run("rd", REPO1_SRC_NAME, "-f")
    run("rd", REPO2_NAME, "-f")
    run("rd", "linked-epel9", "-f")
    run("rl")  # confirm deletion

    # Re-add repos then delete all
    run("ra", REPO1_NAME, REPO1_BASEURL, "-m", REPOMD1_URL)
    run("ra", REPO2_NAME, REPO2_BASEURL, "-m", REPOMD2_URL)
    run("rd", "-A", "-f")
    run("rl")  # confirm deletion

    print("\033[32mAll tests completed successfully.\033[0m")


if __name__ == "__main__":
    main()
