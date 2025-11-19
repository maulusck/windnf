# db_manager.py
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union

from .logger import setup_logger

_logger = setup_logger()


class DbManager:
    def __init__(self, db_path: Path) -> None:
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self._configure_pragmas()
        self._init_schema()

    def _configure_pragmas(self) -> None:
        pragmas = [
            "PRAGMA synchronous = OFF;",
            "PRAGMA journal_mode = MEMORY;",
            "PRAGMA cache_size = 100000;",
            "PRAGMA locking_mode = EXCLUSIVE;",
        ]
        for pragma in pragmas:
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

        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            package_id INTEGER NOT NULL,
            filepath TEXT NOT NULL,
            UNIQUE(package_id, filepath),
            FOREIGN KEY(package_id) REFERENCES packages(id) ON DELETE CASCADE
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

    # Repository Methods

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

    def get_repositories(self) -> List[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM repositories ORDER BY name").fetchall()

    def get_repo_by_name(self, name: str) -> Optional[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM repositories WHERE name = ?", (name,)).fetchone()

    def delete_repository(self, repo_id: int) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM repositories WHERE id = ?", (repo_id,))
        _logger.info(f"Repository id={repo_id} deleted.")

    def clear_repo_packages(self, repo_id: int) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM files WHERE package_id IN (SELECT id FROM packages WHERE repo_id = ?)",
                (repo_id,),
            )
            self.conn.execute(
                "DELETE FROM provides WHERE package_id IN (SELECT id FROM packages WHERE repo_id = ?)",
                (repo_id,),
            )
            self.conn.execute(
                "DELETE FROM requires WHERE package_id IN (SELECT id FROM packages WHERE repo_id = ?)",
                (repo_id,),
            )
            self.conn.execute("DELETE FROM packages WHERE repo_id = ?", (repo_id,))
        _logger.info(f"All packages cleared for repo_id={repo_id}.")

    # Package Methods

    def add_packages_bulk(self, repo_id: int, packages: List[Dict]) -> List[int]:
        if not packages:
            return []

        pkg_data = [
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                pkg_data,
            )

        rows = self.conn.execute("SELECT id FROM packages WHERE repo_id = ?", (repo_id,)).fetchall()
        return [row["id"] for row in rows]

    def add_files(self, package_id: int, files: List[str]) -> None:
        if not files:
            return
        with self.conn:
            self.conn.executemany(
                "INSERT OR IGNORE INTO files(package_id, filepath) VALUES (?, ?)",
                [(package_id, f) for f in files],
            )

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

    # Search and Info Methods

    def search_packages(
        self,
        patterns: Union[str, List[str]],
        repo_names: Optional[List[str]] = None,
        exact_version: bool = False,
    ) -> List[sqlite3.Row]:
        if isinstance(patterns, str):
            patterns = [patterns]

        c = self.conn.cursor()
        conditions = []
        params = []

        for pat in patterns:
            name, version = self._parse_pkg_version(pat)
            if version and exact_version:
                conditions.append("(p.name = ? AND p.version = ?)")
                params.extend([name, version])
            else:
                name_like = name.replace("*", "%")
                if "%" not in name_like:
                    name_like = f"%{name_like}%"
                conditions.append("p.name LIKE ?")
                params.append(name_like)

        if not conditions:
            return []

        where_clause = " OR ".join(conditions)

        repo_filter = ""
        if repo_names:
            placeholders = ",".join("?" for _ in repo_names)
            repo_filter = f"AND r.name IN ({placeholders})"
            params.extend(repo_names)

        sql = f"""
        SELECT p.*, r.name as repo_name
        FROM packages p
        JOIN repositories r ON p.repo_id = r.id
        WHERE ({where_clause}) {repo_filter}
        ORDER BY p.epoch DESC, p.version DESC, p.release DESC, p.name ASC
        """
        c.execute(sql, params)
        return c.fetchall()

    def get_package_info(
        self,
        repo_name: str,
        package_name: str,
        version: Optional[str] = None,
    ) -> Optional[sqlite3.Row]:
        c = self.conn.cursor()
        if version:
            c.execute(
                """
                SELECT p.*, r.name as repo_name FROM packages p
                JOIN repositories r ON p.repo_id = r.id
                WHERE r.name = ? AND p.name = ? AND p.version = ?
                """,
                (repo_name, package_name, version),
            )
        else:
            c.execute(
                """
                SELECT p.*, r.name as repo_name FROM packages p
                JOIN repositories r ON p.repo_id = r.id
                WHERE r.name = ? AND p.name = ?
                ORDER BY p.epoch DESC, p.version DESC, p.release DESC
                LIMIT 1
                """,
                (repo_name, package_name),
            )
        return c.fetchone()

    def get_providing_packages(
        self,
        provide_name: str,
        repo_names: Optional[List[str]] = None,
    ) -> List[sqlite3.Row]:
        c = self.conn.cursor()
        query = """
            SELECT p.*, r.name as repo_name FROM packages p
            JOIN provides pr ON pr.package_id = p.id
            JOIN repositories r ON p.repo_id = r.id
            WHERE pr.provide_name = ?
        """
        params = [provide_name]
        if repo_names:
            placeholders = ",".join("?" for _ in repo_names)
            query += f" AND r.name IN ({placeholders})"
            params.extend(repo_names)
        query += " ORDER BY p.name"
        c.execute(query, params)
        return c.fetchall()

    # Dependency resolution

    def get_direct_dependencies(self, package_id: int, include_weak: bool = False) -> Set[int]:
        c = self.conn.cursor()
        query = """
            SELECT DISTINCT pkg.id
            FROM requires AS req
            LEFT JOIN provides AS prov ON req.require_name = prov.provide_name
            LEFT JOIN packages AS pkg ON (prov.package_id = pkg.id OR req.require_name = pkg.name)
            WHERE req.package_id = ?
        """
        params = [package_id]
        if not include_weak:
            query += " AND req.is_weak = 0"
        c.execute(query, params)
        results = c.fetchall()
        return {row["id"] for row in results if row["id"] is not None}

    def get_dependencies_for_package(
        self,
        package_id: int,
        include_weak: bool = False,
        recurse: bool = False,
    ) -> Set[int]:
        dependencies = set()
        to_process = {package_id}

        while to_process:
            current_id = to_process.pop()
            c = self.conn.cursor()
            query = """
                SELECT p.id FROM requires req
                JOIN packages p ON req.require_name = p.name
                WHERE req.package_id = ?
            """
            params = [current_id]
            if not include_weak:
                query += " AND req.is_weak = 0"

            c.execute(query, params)
            required_ids = {row["id"] for row in c.fetchall()}

            new_ids = required_ids - dependencies
            dependencies.update(new_ids)
            if recurse:
                to_process.update(new_ids)

        return dependencies

    # Utilities

    def _parse_pkg_version(self, pkg_version: str) -> Tuple[str, Optional[str]]:
        """Split package:version string into name and version."""
        if ":" in pkg_version:
            name, version = pkg_version.split(":", 1)
            return name, version
        return pkg_version, None
