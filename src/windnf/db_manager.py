# db_manager.py
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urljoin

from .logger import setup_logger

_logger = setup_logger()


class DbManager:
    def __init__(self, db_path: Path) -> None:
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self._configure_pragmas()
        self._init_schema()

    # -------------------------------
    # Unified Helper
    # -------------------------------
    def _to_dict(self, obj):
        if obj is None:
            return None
        if isinstance(obj, sqlite3.Row):
            return dict(obj)
        if isinstance(obj, (list, tuple)):
            return [dict(r) for r in obj]
        raise TypeError(f"_to_dict(): unsupported type {type(obj)}")

    # -------------------------------
    # Initialization
    # -------------------------------
    def _configure_pragmas(self) -> None:
        for pragma in [
            "PRAGMA synchronous = OFF;",
            "PRAGMA journal_mode = MEMORY;",
            "PRAGMA cache_size = 100000;",
            "PRAGMA locking_mode = EXCLUSIVE;",
            "PRAGMA foreign_keys = ON;",
        ]:
            self.conn.execute(pragma)

    def _init_schema(self) -> None:
        schema_sql = """
        CREATE TABLE IF NOT EXISTS repositories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            base_url TEXT NOT NULL,
            repomd_url TEXT NOT NULL,
            last_updated TEXT
        );

        CREATE TABLE IF NOT EXISTS packages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            version TEXT NOT NULL,
            release TEXT NOT NULL,
            epoch INTEGER DEFAULT 0,
            arch TEXT,
            filepath TEXT NOT NULL,
            summary TEXT,
            description TEXT,
            license TEXT,
            vendor TEXT,
            group_name TEXT,
            buildhost TEXT,
            sourcerpm TEXT,
            header_range_start TEXT,
            header_range_end TEXT,
            packager TEXT,
            url TEXT,
            size_package INTEGER,
            size_installed INTEGER,
            size_archive INTEGER,
            UNIQUE(repo_id, name, version, release, epoch, arch),
            FOREIGN KEY(repo_id) REFERENCES repositories(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS provides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            package_id INTEGER NOT NULL,
            provide_name TEXT NOT NULL,
            UNIQUE(package_id, provide_name),
            FOREIGN KEY(package_id) REFERENCES packages(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS requires (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            package_id INTEGER NOT NULL,
            require_name TEXT NOT NULL,
            is_weak INTEGER DEFAULT 0,
            UNIQUE(package_id, require_name, is_weak),
            FOREIGN KEY(package_id) REFERENCES packages(id) ON DELETE CASCADE
        );
        """
        with self.conn:
            self.conn.executescript(schema_sql)

    # -------------------------------
    # Repository Methods
    # -------------------------------
    def add_repository(self, name: str, base_url: str, repomd_url: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO repositories(name, base_url, repomd_url)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    base_url=excluded.base_url,
                    repomd_url=excluded.repomd_url
                """,
                (name, base_url, repomd_url),
            )
        _logger.info(f"Repository '{name}' added/updated.")

    def update_repo_timestamp(self, repo_id: int, timestamp: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE repositories SET last_updated = ? WHERE id = ?",
                (timestamp, repo_id),
            )

    def get_repositories(self) -> List[Dict]:
        rows = self.conn.execute("SELECT * FROM repositories ORDER BY name").fetchall()
        return self._to_dict(rows)

    def get_repo_map(self) -> Dict[str, Dict]:
        repos = self.get_repositories()
        return {r["name"]: r for r in repos}

    def get_repo_by_name(self, name: str) -> Optional[Dict]:
        row = self.conn.execute("SELECT * FROM repositories WHERE name = ?", (name,)).fetchone()
        return self._to_dict(row)

    def delete_repository(self, repo_name: str) -> None:
        repo = self.get_repo_by_name(repo_name)
        if not repo:
            _logger.warning(f"Repository '{repo_name}' not found.")
            return

        _logger.info(f"Deleting repository '{repo_name}' and all related data...")

        with self.conn:
            # Cascade deletes will automatically remove packages, provides, and requires
            self.conn.execute("DELETE FROM repositories WHERE id = ?", (repo["id"],))

        _logger.info(f"Repository '{repo_name}' deleted (including all packages and dependencies).")

    def clear_repo_packages(self, repo_id: int) -> None:
        sqls = [
            "DELETE FROM provides WHERE package_id IN (SELECT id FROM packages WHERE repo_id = ?)",
            "DELETE FROM requires WHERE package_id IN (SELECT id FROM packages WHERE repo_id = ?)",
            "DELETE FROM packages WHERE repo_id = ?",
        ]
        with self.conn:
            for sql in sqls:
                self.conn.execute(sql, (repo_id,))
        _logger.info(f"All packages cleared for repo_id={repo_id}.")

    # -------------------------------
    # Package Methods
    # -------------------------------
    def add_packages(self, repo_id: int, packages: List[Dict]) -> List[int]:
        if not packages:
            return []

        pkg_rows = [
            (
                repo_id,
                p["name"],
                p["version"],
                p["release"],
                p.get("epoch", 0),
                p.get("arch", "noarch"),
                p.get("filepath"),
                p.get("summary"),
                p.get("description"),
                p.get("license"),
                p.get("vendor"),
                p.get("group"),
                p.get("buildhost"),
                p.get("sourcerpm"),
                p.get("header_range_start"),
                p.get("header_range_end"),
                p.get("packager"),
                p.get("url"),
                p.get("size_package"),
                p.get("size_installed"),
                p.get("size_archive"),
            )
            for p in packages
        ]

        with self.conn:
            self.conn.executemany(
                """
                INSERT OR IGNORE INTO packages(
                    repo_id, name, version, release, epoch, arch, filepath,
                    summary, description, license, vendor, group_name, buildhost,
                    sourcerpm, header_range_start, header_range_end, packager, url,
                    size_package, size_installed, size_archive
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                pkg_rows,
            )

        ids = [r["id"] for r in self.conn.execute("SELECT id FROM packages WHERE repo_id = ?", (repo_id,)).fetchall()]
        return ids

    def add_provides(self, package_id: int, provides: Set[str]) -> None:
        if not provides:
            return
        with self.conn:
            self.conn.executemany(
                "INSERT OR IGNORE INTO provides(package_id, provide_name) VALUES (?, ?)",
                [(package_id, p) for p in provides],
            )

    def add_requires(self, package_id: int, requires: Set[str], is_weak: bool = False) -> None:
        if not requires:
            return
        with self.conn:
            self.conn.executemany(
                "INSERT OR IGNORE INTO requires(package_id, require_name, is_weak) VALUES (?, ?, ?)",
                [(package_id, r, int(is_weak)) for r in requires],
            )

    # -------------------------------
    # Bulk Fetch
    # -------------------------------
    def get_packages_by_ids(self, ids: List[int]) -> Dict[int, Dict]:
        if not ids:
            return {}
        placeholders = ",".join("?" for _ in ids)
        rows = self.conn.execute(f"SELECT * FROM packages WHERE id IN ({placeholders})", ids).fetchall()
        dicts = self._to_dict(rows)
        return {r["id"]: r for r in dicts}

    def get_source_packages_for(self, pkg_ids: List[int]) -> List[Dict]:
        if not pkg_ids:
            return []
        placeholders = ",".join("?" for _ in pkg_ids)
        rows = self.conn.execute(
            f"""
            SELECT p.*, r.name AS repo_name
            FROM packages p
            JOIN repositories r ON p.repo_id = r.id
            WHERE p.sourcerpm IN (
                SELECT name FROM packages WHERE id IN ({placeholders})
            )
            """,
            pkg_ids,
        ).fetchall()
        return self._to_dict(rows)

    def get_package_urls(self, pkg_ids: List[int]) -> Dict[int, str]:
        if not pkg_ids:
            return {}

        repo_map = {r["id"]: r for r in self.get_repositories()}
        placeholders = ",".join("?" for _ in pkg_ids)

        rows = self.conn.execute(
            f"SELECT id, repo_id, filepath, url FROM packages WHERE id IN ({placeholders})",
            pkg_ids,
        ).fetchall()

        result = {}
        for r in rows:
            if r["url"]:
                result[r["id"]] = r["url"]
            else:
                repo = repo_map[r["repo_id"]]
                result[r["id"]] = urljoin(repo["base_url"].rstrip("/") + "/", r["filepath"].lstrip("/"))
        return result

    # -------------------------------
    # Search
    # -------------------------------
    def search_packages(
        self,
        patterns: Union[str, List[str]],
        repo_names: Optional[List[str]] = None,
        exact_version: bool = False,
    ) -> List[Dict]:

        if isinstance(patterns, str):
            patterns = [patterns]

        conditions, params = [], []

        for pat in patterns:
            name, version = self._parse_pkg_version(pat)

            if version and exact_version:
                conditions.append("(p.name = ? AND p.version = ?)")
                params.extend([name, version])
            else:
                like = name.replace("*", "%")
                if "%" not in like:
                    like = f"%{like}%"
                conditions.append("p.name LIKE ?")
                params.append(like)

        if not conditions:
            return []

        where_clause = " OR ".join(conditions)

        repo_filter = ""
        if repo_names:
            placeholders = ",".join("?" for _ in repo_names)
            repo_filter = f"AND r.name IN ({placeholders})"
            params.extend(repo_names)

        sql = f"""
        SELECT p.*, r.name AS repo_name
        FROM packages p
        JOIN repositories r ON p.repo_id = r.id
        WHERE ({where_clause}) {repo_filter}
        ORDER BY p.epoch DESC, p.version DESC, p.release DESC, p.name ASC
        """

        rows = self.conn.execute(sql, params).fetchall()
        return self._to_dict(rows)

    def get_package_info(
        self,
        repo_name: str,
        package_name: str,
        version: Optional[str] = None,
    ) -> Optional[Dict]:

        if version:
            sql = """
            SELECT p.*, r.name AS repo_name
            FROM packages p
            JOIN repositories r ON p.repo_id = r.id
            WHERE r.name = ? AND p.name = ? AND p.version = ?
            """
            row = self.conn.execute(sql, (repo_name, package_name, version)).fetchone()
        else:
            sql = """
            SELECT p.*, r.name AS repo_name
            FROM packages p
            JOIN repositories r ON p.repo_id = r.id
            WHERE r.name = ? AND p.name = ?
            ORDER BY p.epoch DESC, p.version DESC, p.release DESC
            LIMIT 1
            """
            row = self.conn.execute(sql, (repo_name, package_name)).fetchone()

        return self._to_dict(row)

    # -------------------------------
    # Dependency Helpers
    # -------------------------------
    def get_all_packages(self) -> Dict[int, Dict]:
        rows = self.conn.execute("SELECT * FROM packages").fetchall()
        dicts = self._to_dict(rows)
        return {p["id"]: p for p in dicts}

    def get_provides_map(self) -> Dict[str, Set[int]]:
        provides: Dict[str, Set[int]] = {}
        for r in self.conn.execute("SELECT package_id, provide_name FROM provides"):
            provides.setdefault(r["provide_name"], set()).add(r["package_id"])
        return provides

    def get_requires_map(self) -> Dict[int, List[Tuple[str, bool]]]:
        requires: Dict[int, List[Tuple[str, bool]]] = {}
        for r in self.conn.execute("SELECT package_id, require_name, is_weak FROM requires"):
            requires.setdefault(r["package_id"], []).append((r["require_name"], bool(r["is_weak"])))
        return requires

    # -------------------------------
    # Utilities
    # -------------------------------
    def _parse_pkg_version(self, pkg_version: str) -> Tuple[str, Optional[str]]:
        if ":" in pkg_version:
            return pkg_version.split(":", 1)
        return pkg_version, None
