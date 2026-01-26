<h1>
  <img src="https://raw.githubusercontent.com/maulusck/windnf/refs/heads/master/res/windnf.svg"
       alt="winDNF Icon"
       width="100"
       style="vertical-align: middle;">
  winDNF
</h1>

A DNF-like RPM simulator for Windows and other non-Linux systems.

winDNF allows you to download RPM packages and their dependencies from standard
repositories without requiring a Linux environment. It is still a **work in progress**, and many things are not working as they should (see [TODO](https://github.com/maulusck/windnf/blob/master/TODO.md). It is stable for basic usage though.
It works by syncing multiple RPM repositories metadata and querying a single, local DB file in order to resolve packages metadata.

All configuration files and the internal database are stored in your
home directory at `~/.config/windnf/` by default:

- Config file: `~/.config/windnf/windnf.conf`
- Database: `~/.config/windnf/windnf.sqlite`

These paths are editable in the configuration file. winDNF automatically creates this folder and the
default config file on first run.

It also timidly tries to be [NEVRA](https://deepwiki.com/rpm-software-management/hawkey/4.2-nevra-parsing-and-string-utilities) compliant, although with limited success (for now).

---

## Installation

winDNF is distributed as a pip package:

```
pip install windnf
```

Simple as. No further configuration required.

---

## Usage

```
windnf <command> [options] [arguments]
```

Run `windnf <command> --help` for command-specific help.

---

## Common Commands

### Repositories

- **repoadd (ra)** — Add or update a repository.
- **repolink (rlk)** — Link a source repository to a binary repository.
- **repolist (rl)** — List configured repositories.
- **reposync (rs)** — Download and refresh repository metadata.
- **repodel (rd)** — Remove one or more repositories.

### Packages

- **search (s)** — Search for packages by name or pattern.
- **info (i)** — Show detailed package information (NEVRA).
- **resolve (rv)** — Resolve dependencies for one or more packages.
- **download (dl)** — Download packages or source RPMs.

---

## Options (Overview)

Many commands support these common options:

- `-r, --repo, --repoid` — Limit the operation to specific repositories
- `-R, --recursive [DEPTH]` — Perform recursive dependency resolution
  - Used without a value, resolves the full dependency tree
  - When a number is provided, limits recursion depth (e.g. `--recursive 1` for direct dependencies only)
- `--arch <arch>` — Target a specific architecture (not yet supported)

Use `--help` with any command to see all available options.

---

## Configuration

By default, winDNF stores all configuration and the database in:

```
~/.config/windnf/
```

Contents include:

- `windnf.conf` — main configuration file
- `windnf.sqlite` — internal package/repo database

Configurable options in `windnf.conf`:

- `downloader` — method used to download packages (default: `powershell`)
- `skip_ssl_verify` — skip SSL verification when downloading (default: true)
- `db_path` — path to the internal database (default: `~/.config/windnf/windnf.sqlite`)
- `download_path` — default directory for downloaded packages (default: `.`)

You can edit these paths in the config file. winDNF creates missing folders
and files automatically.

---

## Environment

```
WINDNF_DEBUG=1
```

Enable verbose debug output and full tracebacks.

---

## Notes

- winDNF does **not modify your system**.
- It reads repository metadata, resolves dependencies, and downloads RPMs only.
- Designed primarily for **Windows or non-Linux systems** where traditional
  package managers are not available.

---
