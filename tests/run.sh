#!/usr/bin/env bash
set -e
cd "$(dirname ${0})/../"

#WINDNF="python -m windnf.cli"
WINDNF="windnf"
TESTDIR="tests"
REPO1_NAME="epel9"
REPO1_BASEURL="https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/"
REPOMD1_URL="${REPO1_BASEURL}repodata/repomd.xml"

REPO2_NAME="zabbix9"
REPO2_BASEURL="https://repo.zabbix.com/zabbix/7.0/centos/9/x86_64/"
REPOMD2_URL="${REPO2_BASEURL}repodata/repomd.xml"

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
run "$WINDNF repoadd $REPO1_NAME $REPO1_BASEURL"
run "$WINDNF repoadd $REPO1_NAME $REPO1_BASEURL --repomd $REPOMD1_URL"
run "$WINDNF repoadd $REPO2_NAME $REPO2_BASEURL --repomd $REPOMD2_URL"

# ================================================================
# REPOLIST
# ================================================================
run "$WINDNF repolist"

# ================================================================
# REPOSYNC
# ================================================================
run "$WINDNF reposync $REPO1_NAME"
run "$WINDNF reposync $REPO2_NAME"
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
# duplicates
run "$WINDNF search bash --showduplicates"
# repo-filtered search
run "$WINDNF search bash --repo $REPO1_NAME"

# ================================================================
# SEARCH for zabbix-agent2 in different repos
# ================================================================
echo "Running zabbix-agent2 search tests..."
# search in epel9 only
run "$WINDNF search zabbix-agent2 --repo $REPO1_NAME"
# search in zabbix9 only
run "$WINDNF search zabbix-agent2 --repo $REPO2_NAME"
# search in both repos
run "$WINDNF search zabbix-agent2 --repo $REPO1_NAME --repo $REPO2_NAME"
run "$WINDNF search zabbix-agent2 --repo $REPO1_NAME,$REPO2_NAME"
# search in no repo (default behavior, all configured repos)
run "$WINDNF search zabbix-agent2"

# ================================================================
# RESOLVE
# ================================================================
run "$WINDNF resolve bash"
run "$WINDNF resolve bash --recursive"
run "$WINDNF resolve bash --weakdeps"
run "$WINDNF resolve bash --arch x86_64"
run "$WINDNF resolve bash --repo $REPO1_NAME"

# ================================================================
# DOWNLOAD
# ================================================================
run "$WINDNF download bash --urls"
run "$WINDNF download '*ash' --urls"
run "$WINDNF download bash --downloaddir $TESTDIR/downloads --urls"
run "$WINDNF download bash --resolve --urls"
run "$WINDNF download bash --source --urls"
run "$WINDNF download bash --arch x86_64 --urls"
run "$WINDNF download bash --repo $REPO1_NAME --urls"

# ================================================================
# REPODEL
# ================================================================
# delete one
run "$WINDNF repodel $REPO1_NAME --force"
run "$WINDNF repodel $REPO2_NAME --force"
run "$WINDNF repolist"

# re-add to test --all deletion
run "$WINDNF repoadd $REPO1_NAME $REPO1_BASEURL --repomd $REPOMD1_URL"
run "$WINDNF repoadd $REPO2_NAME $REPO2_BASEURL --repomd $REPOMD2_URL"

# delete all
run "$WINDNF repodel --all --force"
run "$WINDNF repolist"

echo
echo -e "\033[32mAll tests completed successfully.\033[0m"
