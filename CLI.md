# windnf CLI Command Reference

## Command Aliases Overview

| Command    | Alias | Description |
|-----------|-------|-------------|
| repoadd    | ra    | Add (or update) a repository |
| repolist   | rl    | List repositories |
| repodel    | rd    | Delete repositories |
| reposync   | rs    | Sync repository metadata |
| repolink   | rlk   | Link source repo → binary repo |
| search     | s     | Search for packages |
| info       | i     | Show full NEVRA package information |
| resolve    | rv    | Resolve dependency sets |
| download   | dl    | Download packages / SRPMs |

---

## Repository Commands

### repoadd (`ra`)
| Category | Name | Aliases | Description |
|---------|------|---------|-------------|
| Positional | `name` | — | Repository unique identifier |
| Positional | `baseurl` | — | Base URL of the repository |
| Option | `--repomd` | `-m` | Repodata path (default: `repodata/repomd.xml`) |
| Option | `--type` | `-t` | Repository type: `binary` or `source` (default: `binary`) |
| Option | `--source-repo` | `-s` | Link an existing source repo |

---

### repolink (`rlk`)
| Category | Name | Aliases | Description |
|---------|------|---------|-------------|
| Positional | `binary_repo` | — | Existing binary repo name |
| Positional | `source_repo` | — | Existing source repo name |

---

### repolist (`rl`)
| Category | Name | Aliases | Description |
|---------|------|---------|-------------|
| — | — | — | List all configured repositories |

---

### reposync (`rs`)
| Category | Name | Aliases | Description |
|---------|------|---------|-------------|
| Positional | `names…` | — | Repository names (optional) |
| Option | `--all` | `-A` | Sync all repositories |

---

### repodel (`rd`)
| Category | Name | Aliases | Description |
|---------|------|---------|-------------|
| Positional | `names…` | — | Repository names |
| Option | `--all` | `-A` | Delete all repositories |
| Option | `--force` | `-f` | Skip confirmation |

---

## Package Queries

### search (`s`)
| Category | Name | Aliases | Description |
|---------|------|---------|-------------|
| Positional | `patterns…` | — | Wildcards or NEVRA |
| Option | `--repo` | `--repoid`, `-r` | Repository selector (comma-separated allowed) |
| Option | `--showduplicates` | — | Show all versions (not only newest) |

---

### info (`i`)
| Category | Name | Aliases | Description |
|---------|------|---------|-------------|
| Positional | `pattern` | — | Name or NEVRA |
| Option | `--repo` | `--repoid`, `-r` | Repository selector |

---

## Dependency Resolution

### resolve (`rv`)
| Category | Name | Aliases | Description |
|---------|------|---------|-------------|
| Positional | `packages…` | — | Name or NEVRA |
| Option | `--repo` | `--repoid`, `-r` | Repository selector |
| Option | `--weakdeps` | `-w` | Include weak dependencies |
| Option | `--recursive` | `-R` | Traverse dependencies recursively |
| Option | `--arch` | — | Architecture filter |

---

## Downloading

### download (`dl`)
| Category | Name | Aliases | Description |
|---------|------|---------|-------------|
| Positional | `packages…` | — | Name/NEVRA/wildcards |
| Option | `--repo` | `--repoid`, `-r` | Repository selector |
| Option | `--downloaddir` | `-x` | Download directory |
| Option | `--destdir` | — | Alias for `--downloaddir` |
| Option | `--resolve` | — | Download dependencies |
| Option | `--recurse` | `-R` | Alias for recursive dependency download |
| Option | `--source` | `-S` | Download SRPMs instead of binary RPMs |
| Option | `--urls` | `--url` | Print URLs only, do not download |
| Option | `--arch` | — | Architecture filter |

