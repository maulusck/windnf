import sqlite3
from pathlib import Path
from typing import List, Optional, Set, Union

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
        with self.conn:
            self.conn.executescript(schema)

    # Repository methods
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

    def update_repo_timestamp(self, repo_id: int, timestamp: str) -> None:
        with self.conn:
            self.conn.execute("UPDATE repositories SET last_updated = ? WHERE id = ?", (timestamp, repo_id))

    def get_repositories(self) -> List[sqlite3.Row]:
        c = self.conn.cursor()
        c.execute("SELECT * FROM repositories ORDER BY name")
        return c.fetchall()

    def get_repo_by_name(self, name: str) -> Optional[sqlite3.Row]:
        c = self.conn.cursor()
        c.execute("SELECT * FROM repositories WHERE name = ?", (name,))
        return c.fetchone()

    def delete_repository(self, repo_id: int) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM repositories WHERE id = ?", (repo_id,))

    def clear_repo_packages(self, repo_id: int) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM provides WHERE package_id IN (SELECT id FROM packages WHERE repo_id = ?)", (repo_id,)
            )
            self.conn.execute(
                "DELETE FROM requires WHERE package_id IN (SELECT id FROM packages WHERE repo_id = ?)", (repo_id,)
            )
            self.conn.execute("DELETE FROM packages WHERE repo_id = ?", (repo_id,))

    # Package methods
    def add_package(
        self, repo_id: int, name: str, version: str, release: str, epoch: int, arch: str, filepath: str
    ) -> int:
        with self.conn:
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO packages(repo_id, name, version, release, epoch, arch, filepath)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (repo_id, name, version, release, epoch, arch, filepath),
            )
            if cur.lastrowid:
                return cur.lastrowid

            cur = self.conn.execute(
                """
                SELECT id FROM packages
                WHERE repo_id = ? AND name = ? AND version = ? AND release = ? AND epoch = ? AND arch = ?
                """,
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
