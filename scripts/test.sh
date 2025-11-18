#!/usr/bin/env bash
set -e

#WINDNF="python -m windnf.cli"
WINDNF="windnf"
TESTDIR="tests"
REPO_NAME="epel9"
REPO_BASEURL="https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/"
REPOMD_URL="${REPO_BASEURL}repodata/repomd.xml"

mkdir -p "$TESTDIR"
mkdir -p "$TESTDIR/downloads"

cyan="\033[36m"
reset="\033[0m"

run() {
    echo -e "${cyan}[CMD]$reset $*"
    eval "$*"
}

echo "Starting windnf test suite..."
echo

# ================================================================
# REPOADD
# ================================================================
run "$WINDNF repoadd $REPO_NAME $REPO_BASEURL --repomd $REPOMD_URL"

# ================================================================
# REPOLIST
# ================================================================
run "$WINDNF repolist"

# ================================================================
# REPOSYNC
# ================================================================
run "$WINDNF reposync $REPO_NAME"
run "$WINDNF reposync --all"

# ================================================================
# SEARCH
# ================================================================
# basic search
run "$WINDNF search bash"

# wildcard searches
run "$WINDNF search '*ash'"
run "$WINDNF search 'bash*'"
run "$WINDNF search '*bash*'"

# OR search (description + URL)
run "$WINDNF search bash --all"

# duplicates
run "$WINDNF search bash --showduplicates"

# repo-filtered search
run "$WINDNF search bash --repo $REPO_NAME"

# ================================================================
# RESOLVE
# ================================================================
run "$WINDNF resolve bash"
run "$WINDNF resolve bash --recursive"
run "$WINDNF resolve bash --weakdeps"
run "$WINDNF resolve bash --arch x86_64"
run "$WINDNF resolve bash --repo $REPO_NAME"

# ================================================================
# DOWNLOAD
# ================================================================
run "$WINDNF download bash --urls"
run "$WINDNF download '*ash' --urls"
run "$WINDNF download bash --downloaddir $TESTDIR/downloads --urls"
run "$WINDNF download bash --resolve --urls"
run "$WINDNF download bash --source --urls"
run "$WINDNF download bash --arch x86_64 --urls"
run "$WINDNF download bash --repo $REPO_NAME --urls"

# ================================================================
# REPODEL
# ================================================================
# delete one
run "$WINDNF repodel $REPO_NAME --force"
run "$WINDNF repolist"

# re-add to test --all
run "$WINDNF repoadd $REPO_NAME $REPO_BASEURL --repomd $REPOMD_URL"

# delete all
run "$WINDNF repodel --all --force"
run "$WINDNF repolist"

echo
echo -e "\033[32mAll tests completed successfully.\033[0m"
