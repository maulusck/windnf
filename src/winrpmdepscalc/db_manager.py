import logging
import sqlite3
from pathlib import Path
from typing import List, Optional, Set

_logger = logging.getLogger("winrpmdepscalc")


class DbManager:
    def __init__(self, db_path: Path) -> None:
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row

        self.conn.execute("PRAGMA synchronous = OFF;")
        self.conn.execute("PRAGMA journal_mode = MEMORY;")
        self.conn.execute("PRAGMA cache_size = 100000;")
        self.conn.execute("PRAGMA locking_mode = EXCLUSIVE;")

        self._init_schema()

    def _init_schema(self) -> None:
        schema = """
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
            version TEXT,
            release TEXT,
            epoch INTEGER DEFAULT 0,
            arch TEXT,
            filepath TEXT,
            UNIQUE(repo_id, name, version, release, epoch, arch),
            FOREIGN KEY (repo_id) REFERENCES repositories(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS provides (
            package_id INTEGER NOT NULL,
            provide_name TEXT NOT NULL,
            FOREIGN KEY (package_id) REFERENCES packages(id) ON DELETE CASCADE,
            UNIQUE(package_id, provide_name)
        );
        CREATE TABLE IF NOT EXISTS requires (
            package_id INTEGER NOT NULL,
            require_name TEXT NOT NULL,
            is_weak BOOLEAN DEFAULT 0,
            FOREIGN KEY (package_id) REFERENCES packages(id) ON DELETE CASCADE,
            UNIQUE(package_id, require_name, is_weak)
        );
        """
        self.conn.executescript(schema)
        self.conn.commit()

    def add_repository(self, name: str, base_url: str, repomd_url: str) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO repositories(name, base_url, repomd_url) VALUES (?, ?, ?)",
                (name, base_url, repomd_url),
            )

    def get_repositories(self) -> List[sqlite3.Row]:
        c = self.conn.cursor()
        c.execute("SELECT * FROM repositories ORDER BY name")
        return c.fetchall()

    def get_repo_by_name(self, name: str) -> Optional[sqlite3.Row]:
        c = self.conn.cursor()
        c.execute("SELECT * FROM repositories WHERE name=?", (name,))
        return c.fetchone()

    def delete_repository(self, repo_id: int) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM repositories WHERE id = ?", (repo_id,))

    def clear_repo_packages(self, repo_id: int) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM provides WHERE package_id IN (SELECT id FROM packages WHERE repo_id=?)",
                (repo_id,),
            )
            self.conn.execute(
                "DELETE FROM requires WHERE package_id IN (SELECT id FROM packages WHERE repo_id=?)",
                (repo_id,),
            )
            self.conn.execute("DELETE FROM packages WHERE repo_id=?", (repo_id,))

    def add_package(
        self, repo_id: int, name: str, version: str, release: str, epoch: int, arch: str, filepath: str
    ) -> int:
        with self.conn:
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO packages(repo_id, name, version, release, epoch, arch, filepath) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (repo_id, name, version, release, epoch, arch, filepath),
            )
            if cur.lastrowid:
                return cur.lastrowid
            # If already exists, fetch existing id
            cur = self.conn.execute(
                "SELECT id FROM packages WHERE repo_id=? AND name=? AND version=? AND release=? AND epoch=? AND arch=?",
                (repo_id, name, version, release, epoch, arch),
            )
            row = cur.fetchone()
            return row["id"] if row else 0

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

    def search_packages(self, pattern: str, repo_names: Optional[List[str]] = None) -> List[sqlite3.Row]:
        c = self.conn.cursor()
        like_pattern = pattern.replace("*", "%")
        query = (
            "SELECT p.*, r.name as repo_name FROM packages p "
            "JOIN repositories r ON p.repo_id = r.id "
            "WHERE p.name LIKE ?"
        )
        params = [like_pattern]
        if repo_names:
            placeholders = ",".join("?" for _ in repo_names)
            query += f" AND r.name IN ({placeholders})"
            params.extend(repo_names)
        query += " ORDER BY p.name"
        c.execute(query, params)
        return c.fetchall()

    def get_package_by_name_repo(self, repo_name: str, package_name: str) -> Optional[sqlite3.Row]:
        c = self.conn.cursor()
        c.execute(
            "SELECT p.*, r.name as repo_name FROM packages p "
            "JOIN repositories r ON p.repo_id = r.id "
            "WHERE r.name = ? AND p.name = ?",
            (repo_name, package_name),
        )
        return c.fetchone()

    def get_base_url_for_package(self, package_id: int) -> Optional[str]:
        c = self.conn.cursor()
        c.execute(
            "SELECT r.base_url FROM repositories r JOIN packages p ON r.id = p.repo_id WHERE p.id = ?",
            (package_id,),
        )
        row = c.fetchone()
        return row["base_url"] if row else None

    def get_package_info_by_id(self, package_id: int) -> Optional[sqlite3.Row]:
        c = self.conn.cursor()
        c.execute(
            "SELECT p.name, r.name as repo_name, p.version, p.release, p.epoch, p.arch "
            "FROM packages p JOIN repositories r ON p.repo_id = r.id WHERE p.id = ?",
            (package_id,),
        )
        return c.fetchone()

    def get_dependencies_for_package(self, package_id: int, include_weak: bool = False) -> List[str]:
        """
        Return list of require names for given package.
        """
        c = self.conn.cursor()
        query = "SELECT require_name FROM requires WHERE package_id = ?"
        params = [package_id]
        if not include_weak:
            query += " AND is_weak = 0"
        c.execute(query, params)
        return [row[0] for row in c.fetchall()]

    def get_required_package_ids(self, package_id: int, include_weak: bool) -> Set[int]:
        """
        Return package IDs required by package_id (join requires to packages by name).
        """
        c = self.conn.cursor()
        query = (
            "SELECT p.id FROM requires req " "JOIN packages p ON req.require_name = p.name " "WHERE req.package_id = ?"
        )
        params = [package_id]
        if not include_weak:
            query += " AND req.is_weak = 0"
        c.execute(query, params)
        return {row["id"] for row in c.fetchall()}

    def get_packages_providing(self, provide_name: str, repo_names: Optional[List[str]] = None) -> List[sqlite3.Row]:
        """
        Return list of packages providing the given capability.
        """
        c = self.conn.cursor()
        query = (
            "SELECT p.*, r.name as repo_name FROM packages p "
            "JOIN provides pr ON pr.package_id = p.id "
            "JOIN repositories r ON p.repo_id = r.id "
            "WHERE pr.provide_name = ?"
        )
        params = [provide_name]
        if repo_names:
            placeholders = ",".join("?" for _ in repo_names)
            query += f" AND r.name IN ({placeholders})"
            params.extend(repo_names)
        query += " ORDER BY p.name"
        c.execute(query, params)
        return c.fetchall()
