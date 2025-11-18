# winDNF CLI Project Overview

This document provides a comprehensive explanation of the **winDNF CLI project**, including its purpose, functionality, and design philosophy. It is written in a way that an AI or a new developer can understand the project as a whole without diving into implementation details.

---

## Project Purpose

The **winDNF CLI** is a command-line interface tool designed to manage software repositories and packages in a **DNF/YUM-style workflow**. Its main goals are:

1. **Repository Management:** Add, remove, list, and synchronize repositories.
2. **Package Discovery:** Search packages across configured repositories.
3. **Dependency Resolution:** Resolve package dependencies with optional recursive and weak dependency support.
4. **Package Downloading:** Download packages and their dependencies, with flexible options for architecture, source/binary packages, and destination directories.

The tool is **repository-agnostic**, meaning it works with multiple repositories defined by the user, rather than being tied to a single package source.

---

## Key Concepts

### Repositories
- A repository is a collection of packages hosted at a given URL.
- Each repository is uniquely identified by a `name`.
- Metadata about packages is stored in `repomd.xml` or similar files.
- Users can configure multiple repositories and control which ones are used for searches, dependency resolution, and downloads.

### Packages
- Packages are the individual software components.
- The tool supports searching by **name, description, URL, or globs/wildcards**.
- Packages can be **binary RPMs** or **source RPMs (SRPMs)**.
- Dependency management is handled optionally recursively and can include weak/optional dependencies.

---

## Commands Overview

The CLI is structured into **subcommands** with consistent patterns:

### 1. `repoadd`
- Adds a new repository.
- Requires:
  - `name` — unique identifier.
  - `baseurl` — location of repository files.
- Optional:
  - `--repomd` — path to repository metadata XML.

### 2. `repolist`
- Lists all configured repositories.
- Useful to check which repositories are currently active.

### 3. `reposync`
- Synchronizes local metadata with remote repository.
- Options:
  - Specify repository names or use `--all` to sync all.

### 4. `repodel`
- Deletes repositories and optionally all their packages.
- Options:
  - `--all` — delete all repositories.
  - `--force` — force deletion without confirmation.

### 5. `search`
- Searches packages by pattern.
- Options:
  - `--all` — include description and URL, use OR matching.
  - `--showduplicates` — show all versions of each package.
  - `--repo` — specify repositories (can be repeated, comma-separated).

### 6. `resolve`
- Resolves package dependencies.
- Options:
  - `--weakdeps` — include optional/weak dependencies.
  - `--recursive` — resolve dependencies recursively.
  - `--arch` — target architecture.
  - `--repo` — restrict to specific repositories.

### 7. `download`
- Downloads packages (binary or source) with optional dependency resolution.
- Options:
  - `--downloaddir` or `--destdir` — specify destination directory.
  - `--resolve` — download all dependencies.
  - `--source` — download source RPMs instead of binaries.
  - `--urls` — print download URLs instead of downloading.
  - `--arch` — specify architecture.
  - `--repo` — restrict to specific repositories.

---

## CLI Design Principles

1. **DNF-style commands:** Each command behaves similarly to a DNF/YUM subcommand for familiarity.
2. **Extensible:** Supports multiple repositories, flexible search, and downloads.
3. **Declarative Options:** Users specify what to do (add repo, download package) and the CLI handles orchestration.
4. **Validation:** Conflicting or missing options are detected and reported (e.g., `--all` cannot be combined with specific repo names).

---

## Parameter Handling

- **Repository IDs (`repoids`)**:
  - Can be provided multiple times, and as comma-separated lists.
  - Flattened into a single list internally for consistent processing.
- **Directory Options (`downloaddir` / `destdir`)**:
  - Aliases supported for convenience.
- **Boolean Flags**:
  - `--all`, `--force`, `--recursive`, `--weakdeps`, `--showduplicates`, `--source`, `--urls`
  - These flags toggle specific behavior without requiring a value.

---

## Summary

The **winDNF CLI** is a repository and package management tool that provides:

- Repository lifecycle management (add, list, sync, delete)
- Flexible package search and dependency resolution
- Advanced downloading features (dependencies, architecture, source/binary)
- DNF-style command structure for familiarity and consistency

It is designed to be **user-friendly, flexible, and extensible**, allowing users to handle repositories and packages efficiently in a scripted or interactive environment.

---