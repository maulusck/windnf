#!/usr/bin/env python3
import io
import os
import shutil
import sys
from contextlib import redirect_stdout
from pathlib import Path

from windnf import cli

# ===========================================================
# ANSI colors
# ===========================================================
C_RESET = "\033[0m"
C_RED = "\033[31m"
C_GREEN = "\033[32m"
C_YELLOW = "\033[33m"
C_CYAN = "\033[36m"
C_BOLD = "\033[1m"

# ===========================================================
# Paths
# ===========================================================
ROOT = Path(__file__).parent.resolve()
os.chdir(ROOT)

RESULTS_FILE = ROOT / "tests.txt"
DOWNLOAD_DIR = ROOT / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

# ===========================================================
# Counters
# ===========================================================
TOTAL = PASSED = FAILED = XFAILED = 0
RESULT_LINES = []
ABORTED = False


# ===========================================================
# Helpers
# ===========================================================
def sep(char="="):
    width = shutil.get_terminal_size().columns
    print(char * (width - 1))


def record(name, status):
    RESULT_LINES.append(f"{name} -> {status}")


def flush_results():
    RESULTS_FILE.write_text("\n".join(RESULT_LINES) + "\n")


def run(*args, expect=0):
    """
    Run a CLI command.
    Always prints full output.
    Ctrl+C safe.
    """
    global TOTAL, PASSED, FAILED, XFAILED, ABORTED
    TOTAL += 1

    name = " ".join(args)
    print(f"\n{C_CYAN}{C_BOLD}[TEST]{C_RESET} {name}")
    sep("-")

    buf = io.StringIO()
    exit_code = 0
    old_argv = sys.argv

    try:
        sys.argv = ["windnf"] + list(args)
        with redirect_stdout(buf):
            cli.main()

    except KeyboardInterrupt:
        ABORTED = True
        print(f"\n{C_YELLOW}{C_BOLD}⚠ ABORTED by user{C_RESET}")
        raise

    except SystemExit as e:
        exit_code = e.code if isinstance(e.code, int) else 1

    finally:
        sys.argv = old_argv

    output = buf.getvalue()
    if output.strip():
        print(output.rstrip())

    sep("-")

    if exit_code == expect:
        if expect == 0:
            PASSED += 1
            record(name, "PASS")
            print(f"{C_GREEN}{C_BOLD}✔ PASS{C_RESET}")
        else:
            XFAILED += 1
            record(name, "XFAIL")
            print(f"{C_YELLOW}{C_BOLD}✔ XFAIL{C_RESET}")
    else:
        FAILED += 1
        record(name, "FAIL")
        print(f"{C_RED}{C_BOLD}✘ FAIL{C_RESET} " f"(exit {exit_code}, expected {expect})")

    sys.stdout.flush()


# ===========================================================
# Main test plan
# ===========================================================
def main():
    global ABORTED
    RESULTS_FILE.write_text("")

    print(f"{C_BOLD}windnf manual CLI test suite{C_RESET}")
    print(f"Working dir: {ROOT}")

    try:
        # ===================================================
        # 1. CLI SANITY
        # ===================================================
        sep()
        print(f"{C_BOLD}CLI SANITY{C_RESET}")
        sep()

        run("--version")
        run("--help", expect=0)
        run("nosuchcommand", expect=2)

        # ===================================================
        # 2. REPOSITORY SETUP
        # ===================================================
        sep()
        print(f"{C_BOLD}REPOSITORY SETUP{C_RESET}")
        sep()

        run(
            "repoadd",
            "centos9-baseos",
            "https://mirror.stream.centos.org/9-stream/BaseOS/x86_64/os/",
            "-m",
            "repodata/repomd.xml",
        )

        run(
            "repoadd",
            "centos9-appstream",
            "https://mirror.stream.centos.org/9-stream/AppStream/x86_64/os/",
            "-m",
            "repodata/repomd.xml",
        )

        run(
            "repoadd",
            "epel9",
            "https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/",
            "-m",
            "repodata/repomd.xml",
        )

        run(
            "repoadd",
            "epel9-source",
            "https://dl.fedoraproject.org/pub/epel/9/Everything/source/tree/",
            "-t",
            "source",
            "-m",
            "repodata/repomd.xml",
        )

        # ===================================================
        # 3. REPOLIST / REPOLINK
        # ===================================================
        sep()
        print(f"{C_BOLD}REPO INSPECTION{C_RESET}")
        sep()

        run("repolist")
        run("repolink", "centos9-appstream", "epel9-source")
        run("repolink", "nosuchrepo", "epel9-source", expect=1)

        # ===================================================
        # 4. METADATA SYNC
        # ===================================================
        sep()
        print(f"{C_BOLD}METADATA SYNC{C_RESET}")
        sep()

        run("reposync", "-A")
        run("reposync", "nosuchrepo", expect=1)

        # ===================================================
        # 5. PACKAGE QUERIES
        # ===================================================
        sep()
        print(f"{C_BOLD}PACKAGE QUERIES{C_RESET}")
        sep()

        run("search", "bash")
        run("search", "bash", "--repo", "centos9-baseos")
        run("info", "bash")
        run("info", "bash", "--repo", "centos9-baseos")
        run("info", "nosuchpackage", expect=1)

        # ===================================================
        # 6. DEPENDENCY RESOLUTION
        # ===================================================
        sep()
        print(f"{C_BOLD}DEPENDENCY RESOLUTION{C_RESET}")
        sep()

        run("resolve", "bash")
        run("resolve", "bash", "--recursive")
        run("resolve", "bash", "--recursive", "2")
        run("resolve", "bash", "--weakdeps")
        run("resolve", "nosuchpackage", expect=1)

        # ===================================================
        # 7. DOWNLOADS
        # ===================================================
        sep()
        print(f"{C_BOLD}DOWNLOADS{C_RESET}")
        sep()

        run("download", "bash", "--destdir", str(DOWNLOAD_DIR))
        run("download", "bash", "--urls")
        run("download", "bash", "--resolve")
        run("download", "bash", "--recurse", "1")
        run("download", "bash", "--source")
        run("download", "nosuchpackage", expect=1)

        # ===================================================
        # 8. DESTRUCTIVE OPS (LAST)
        # ===================================================
        sep()
        print(f"{C_BOLD}REPO DELETION{C_RESET}")
        sep()

        run("repodel", "epel9-source")
        run("repodel", "epel9", "--force")
        run("repodel", "--all", "--force")

    except KeyboardInterrupt:
        pass

    finally:
        sep("=")
        flush_results()

        status = (
            f"{C_YELLOW}ABORTED{C_RESET}"
            if ABORTED
            else f"{C_RED}FAILED{C_RESET}" if FAILED else f"{C_GREEN}PASSED{C_RESET}"
        )

        print(
            f"\n{C_BOLD}Summary:{C_RESET} "
            f"{C_GREEN}{PASSED} passed{C_RESET}, "
            f"{C_YELLOW}{XFAILED} xfail{C_RESET}, "
            f"{C_RED}{FAILED} failed{C_RESET}, "
            f"{TOTAL} total"
        )

        print(f"Status: {status}")
        print(f"Recap written to {RESULTS_FILE}")

        if ABORTED:
            sys.exit(130)
        if FAILED:
            sys.exit(1)


if __name__ == "__main__":
    main()
