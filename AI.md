# winDNF CLI Project Overview

This document provides a comprehensive explanation of the **winDNF CLI project**, including its purpose, functionality, NEVRA-aware package handling, and integrated repodata database support. It is written so that an AI or a new developer can understand the project holistically without diving into implementation details.

---

## Project Purpose

The **winDNF CLI** is a command-line interface tool designed to manage software repositories and packages in a **DNF/YUM-style workflow**. Its main goals are:

1. **Repository Management:** Add, remove, list, and synchronize repositories.
2. **Package Discovery:** Search packages across configured repositories with NEVRA awareness.
3. **Dependency Resolution:** Resolve dependency trees with optional recursive and weak dependency support.
4. **Package Downloading:** Download packages and their dependencies, supporting architecture filtering and SRPM/Binary RPM selection.
5. **Repodata Integration:** Import and unify metadata from both `primary.sqlite` and `primary.xml` sources into a consistent internal database.

The tool is **repository-agnostic**, supporting any number of independently defined repositories.

---

## Key Concepts

### Repositories
- A repository is a collection of packages hosted at a specified URL.
- Each repository has a unique `name`.
- Metadata is obtained through `repomd.xml`, which may reference:
  - **primary.sqlite** (SQLite metadata)
  - **primary.xml** (XML metadata)
- winDNF supports **both formats seamlessly**, importing them into a unified internal metadata database.
- The internal DB schema matches standard DNF/YUM metadata:
  - Packages, Provides, Requires, Files, Conflicts, Obsoletes, etc.
  - **Plus an extra `repositories` table** to track package → repo origin.

### Packages
- Packages are represented and compared using full **NEVRA** fields:
  - **N**ame  
  - **E**poch  
  - **V**ersion  
  - **R**elease  
  - **A**rchitecture  
- winDNF supports searching by:
  - NEVRA
  - Name or globs
  - Description
  - URL
- Both **binary RPMs** and **source RPMs (SRPMs)** are supported.
- Dependency management supports recursive expansion and optional weak dependencies.

---

## Commands Overview

The CLI follows a DNF-style subcommand pattern, providing a clear set of actions:

### Repository Commands
- **`repoadd`** — Add a new repository (name + baseurl). Supports automatic repodata detection and optional repomd override.
- **`repolist`** — Display all configured repositories.
- **`reposync`** — Fetch and import repository metadata (SQLite or XML). Supports syncing specific repos or all repos at once.
- **`repodel`** — Remove repositories and optionally delete their stored metadata.
- **`repolink`** — Associate a binary repository with a corresponding source repository.

### Package Query Commands
- **`search`** — Search packages by name, NEVRA, or wildcard patterns. Supports duplicate display and repo filtering.
- **`info`** — Show detailed NEVRA package information, including dependencies and repo origin.

### Dependency & Resolution Commands
- **`resolve`** — Compute dependencies for one or more packages. Supports weak deps, recursive resolution, architecture filtering, and repo selection.

### Download Commands
- **`download`** — Download packages or SRPMs. Supports dependency downloading, listing URLs without downloading, architecture constraints, and selecting output directories.

---

## Repodata & Database Integration

winDNF imports repository metadata into a unified internal database for fast, consistent operations.

### Supported Formats
- **primary.sqlite**
  - Imported using SQLite readers directly.
- **primary.xml**
  - Parsed and converted into the same schema as SQLite metadata.

### Unified Metadata Schema
The DB mirrors standard DNF metadata tables:
- Packages  
- Provides / Requires  
- Conflicts / Obsoletes  
- Files  
- Etc.

With an additional:
- **`repositories` table**, mapping each package record to its originating repository.

This enables:
- Fast search  
- Accurate NEVRA comparisons  
- Cross-repository dependency resolution  
- Deterministic package matching  

---

## CLI Design Principles

1. **DNF-like behavior:** Commands follow familiar patterns for users from RPM-based ecosystems.
2. **NEVRA-accurate operations:** All version comparisons and dependency matches use full NEVRA semantics.
3. **Format-agnostic repodata importing:** SQLite or XML repos behave identically after import.
4. **Extensibility:** Multiple repos, flexible search modes, advanced resolution logic.
5. **Strong validation:** Detects invalid combinations such as `--all` + specific repo names.

---

## Parameter Handling

- **Repository Selectors**
  - Multiple `--repo` values permitted.
  - Comma-separated lists supported.
- **Directory Options**
  - `--downloaddir` and `--destdir` are interchangeable.
- **Boolean Flags**
  - `--all`, `--force`, `--recursive`, `--weakdeps`, `--source`,  
    `--urls`, `--showduplicates`

These influence behavior without requiring explicit values.

---

## Summary

The **winDNF CLI** is a flexible, NEVRA-aware package and repository management tool designed for multi-repository workflows. It provides:

- Full repository lifecycle operations
- Unified metadata ingestion from SQLite or XML repodata
- Accurate NEVRA-based search and dependency resolution
- Robust downloading capabilities
- A familiar DNF-style interface

It aims to be **user-friendly, fast, and extensible**, suitable for automated workflows and interactive use alike.

