import io
import os
import shutil
import sys
from contextlib import redirect_stdout
from pathlib import Path

from windnf import cli

# -----------------------------------------------------------
# Setup test directories
# -----------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()
os.chdir(SCRIPT_DIR)

# Temporary download directory to avoid messing up the actual environment
DOWNLOAD_DIR = SCRIPT_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

# -----------------------------------------------------------
# Repository definitions (CentOS 9 AppStream, BaseOS, EPEL9, and EPEL9 Source)
# -----------------------------------------------------------
REPO1_NAME = "centos9-appstream"
REPO1_BASEURL = "https://mirror.stream.centos.org/9-stream/AppStream/x86_64/os/"
REPOMD1_URL = f"{REPO1_BASEURL}repodata/repomd.xml"

REPO2_NAME = "centos9-baseos"
REPO2_BASEURL = "https://mirror.stream.centos.org/9-stream/BaseOS/x86_64/os/"
REPOMD2_URL = f"{REPO2_BASEURL}repodata/repomd.xml"

REPO3_NAME = "epel9"
REPO3_BASEURL = "https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/"
REPOMD3_URL = f"{REPO3_BASEURL}repodata/repomd.xml"

REPO4_NAME = "epel9-source"
REPO4_BASEURL = "https://dl.fedoraproject.org/pub/epel/9/Everything/source/tree/"
REPOMD4_URL = f"{REPO4_BASEURL}repodata/repomd.xml"


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

    # Force flush after the command to ensure terminal output consistency
    sys.stdout.flush()  # This forces Python to flush its internal buffer

    return out


# -----------------------------------------------------------
# Print separator in red color with terminal width
# -----------------------------------------------------------
def print_separator():
    terminal_width = shutil.get_terminal_size().columns
    separator = "*" * (terminal_width - 1)  # Subtract 1 for terminal's edge
    print(f"\033[31m{separator}\033[0m")  # Red separator


# -----------------------------------------------------------
# Main test execution
# -----------------------------------------------------------
def main():
    print(f"Starting windnf test suite in {SCRIPT_DIR}...\n")

    # ====================================================
    # REPOADD — Add CentOS 9 AppStream, BaseOS, EPEL9, and EPEL9 Source
    # ====================================================
    print_separator()
    run("repoadd", REPO1_NAME, REPO1_BASEURL, "-m", REPOMD1_URL)
    run("repoadd", REPO2_NAME, REPO2_BASEURL, "-m", REPOMD2_URL)
    run("repoadd", REPO3_NAME, REPO3_BASEURL, "-m", REPOMD3_URL)
    run("repoadd", REPO4_NAME, REPO4_BASEURL, "-t", "source", "-m", REPOMD4_URL)

    # Auto-link source repo at add time
    run("repoadd", "linked-epel9", REPO3_BASEURL, "-m", REPOMD3_URL, "-s", REPO4_NAME)

    # ====================================================
    # REPOLINK — Link the repositories
    # ====================================================
    print_separator()
    run("repolink", REPO1_NAME, REPO4_NAME)  # AppStream → EPEL9 Source
    run("repolink", REPO2_NAME, REPO4_NAME)  # BaseOS → EPEL9 Source
    run("repolink", "notarepo", REPO4_NAME)  # Invalid repo link

    # ====================================================
    # REPOLIST — List repositories
    # ====================================================
    print_separator()
    run("repolist")

    # ====================================================
    # REPOSYNC — Sync the repositories
    # ====================================================
    print_separator()
    run("reposync", REPO1_NAME)
    run("reposync", REPO2_NAME)
    run("reposync", REPO3_NAME)
    run("reposync", REPO4_NAME)
    run("reposync", "notarepo")  # Invalid repo sync
    run("reposync", "-A")  # Sync all repositories

    # ====================================================
    # SEARCH — Search for packages in repositories
    # ====================================================
    print_separator()
    patterns = ["bash", "*ash", "bash*", "*bash*"]
    for p in patterns:
        run("search", p)

    run("search", "bash", "--showduplicates")
    run("search", "bash", "-r", REPO1_NAME)
    run("search", "bash", "-r", REPO2_NAME)
    run("search", "bash", "-r", REPO3_NAME)
    run("search", "bash", "-r", REPO4_NAME)
    run("search", "bash", "-r", "notarepo")  # Invalid repo search

    # ====================================================
    # INFO — Package information
    # ====================================================
    print_separator()
    run("info", "bash")
    run("info", "bash", "-r", REPO1_NAME)
    run("info", "bash", "-r", REPO2_NAME)
    run("info", "bash", "-r", REPO3_NAME)
    run("info", "bash", "-r", REPO4_NAME)
    run("info", "bash", "-r", "notarepo")  # Invalid repo info

    # ====================================================
    # RESOLVE — Dependency resolution
    # ====================================================
    print_separator()
    run("resolve", "vlc")
    run("resolve", "vlc", "-R")  # Recursive resolution
    run("resolve", "vlc", "-w")  # Weak dependencies
    run("resolve", "vlc", "--arch", "x86_64")
    run("resolve", "vlc", "--arch", "arm64")
    run("resolve", "vlc", "-r", REPO1_NAME)
    run("resolve", "vlc", "-r", REPO2_NAME)
    run("resolve", "vlc", "-r", REPO3_NAME)
    run("resolve", "vlc", "-r", REPO4_NAME)
    run("resolve", "vlc", "-r", "notarepo")  # Invalid repo resolve

    # ====================================================
    # DOWNLOAD — Download packages, SRPMs, dependencies
    # ====================================================
    print_separator()
    run("download", "vlc", "--urls")
    run("download", "vlc-plugin*", "--urls")
    run("download", "vlc", "-x", str(DOWNLOAD_DIR), "--urls")
    run("download", "vlc", "--resolve", "-x", str(DOWNLOAD_DIR), "--urls")
    run("download", "vlc", "-S", "-x", str(DOWNLOAD_DIR), "--urls")
    run("download", "bash", "-S", "-x", str(DOWNLOAD_DIR), "--urls")
    run("download", "vlc", "--arch", "x86_64", "-x", str(DOWNLOAD_DIR), "--urls")
    run("download", "vlc", "-r", REPO1_NAME, "-x", str(DOWNLOAD_DIR), "--urls")
    run("download", "vlc", "-r", REPO2_NAME, "-S", "-x", str(DOWNLOAD_DIR), "--urls")

    # ====================================================
    # REPODEL — Remove repositories
    # ====================================================
    print_separator()
    run("repodel", REPO1_NAME, "-f")
    run("repodel", REPO2_NAME, "-f")
    run("repodel", REPO3_NAME, "-f")
    run("repodel", REPO4_NAME, "-f")
    run("repodel", "linked-epel9", "-f")
    run("repolist")  # Confirm deletion

    # Re-add repos and delete all
    run("repoadd", REPO1_NAME, REPO1_BASEURL, "-m", REPOMD1_URL)
    run("repoadd", REPO2_NAME, REPO2_BASEURL, "-m", REPOMD2_URL)
    run("repoadd", REPO3_NAME, REPO3_BASEURL, "-m", REPOMD3_URL)
    run("repoadd", REPO4_NAME, REPO4_BASEURL, "-t", "source", "-m", REPOMD4_URL)
    run("repodel", "-A", "-f")
    run("repolist")  # Confirm deletion

    print("\033[32mTest suite complete!\033[0m")


if __name__ == "__main__":
    main()
