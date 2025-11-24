# db_manager.py
from __future__ import annotations

import os
import sqlite3
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from .config import Config
from .logger import setup_logger
from .nevra import NEVRA

_logger = setup_logger()


class DbManager:
    """
    Unified repodata DB manager.

    - Accepts Config instance in constructor (Option A).
    - Manages repositories, packages and relations.
    - Provides an `import_repodb` method that imports a repodata sqlite file (path) into the unified DB.
    """

    def __init__(self, config: Config, schema_path: Optional[Union[str, Path]] = None):
        self.config = config
        self.db_path = Path(self.config.db_path)
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        # Use Row for dict-like access
        self.conn.row_factory = sqlite3.Row
        self._configure_pragmas()

        # load schema if present
        schema_file = Path(schema_path) if schema_path else (Path(__file__).parent / "schema.sql")
        if schema_file.exists():
            with open(schema_file, "r", encoding="utf-8") as fh:
                self.conn.executescript(fh.read())
        else:
            _logger.debug("Schema file not found at %s — assuming DB already initialized.", schema_file)

    # -------------------------
    # PRAGMA tuning
    # -------------------------
    def _configure_pragmas(self) -> None:
        # Good defaults for local single-process ingestion; WAL can be enabled if needed.
        pragmas = (
            "PRAGMA foreign_keys=ON;",
            "PRAGMA synchronous=NORMAL;",
            "PRAGMA journal_mode=WAL;",
            "PRAGMA cache_size=100000;",
            "PRAGMA temp_store=MEMORY;",
        )
        cur = self.conn.cursor()
        for p in pragmas:
            try:
                cur.execute(p)
            except Exception:
                _logger.debug("PRAGMA failed: %s", p)
        cur.close()

    # -------------------------
    # Repository CRUD
    # -------------------------
    def add_repo(
        self,
        name: str,
        base_url: str,
        repomd_url: str,
        rtype: str = "binary",
        source_repo_id: Optional[int] = None,
    ) -> int:
        if rtype not in ("binary", "source"):
            raise ValueError("rtype must be 'binary' or 'source'")

        sql = """
        INSERT INTO repositories (name, base_url, repomd_url, type, source_repo_id)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            base_url=excluded.base_url,
            repomd_url=excluded.repomd_url,
            type=excluded.type,
            source_repo_id=excluded.source_repo_id
        """
        with self.conn:
            self.conn.execute(sql, (name, base_url, repomd_url, rtype, source_repo_id))
        row = self.conn.execute("SELECT id FROM repositories WHERE name=?", (name,)).fetchone()
        return int(row["id"])

    def list_repos(self) -> List[Dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM repositories ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    def get_repo(self, name: Union[str, int]) -> Optional[Dict[str, Any]]:
        if isinstance(name, int):
            r = self.conn.execute("SELECT * FROM repositories WHERE id=?", (name,)).fetchone()
        else:
            r = self.conn.execute("SELECT * FROM repositories WHERE name=?", (name,)).fetchone()
        return dict(r) if r else None

    def delete_repo(self, name_or_id: Union[str, int]) -> bool:
        if isinstance(name_or_id, int):
            row = self.conn.execute("SELECT id FROM repositories WHERE id=?", (name_or_id,)).fetchone()
        else:
            row = self.conn.execute("SELECT id FROM repositories WHERE name=?", (name_or_id,)).fetchone()
        if not row:
            return False
        with self.conn:
            self.conn.execute("DELETE FROM repositories WHERE id=?", (row["id"],))
        return True

    def link_source(self, binary_repo: str, source_repo: str) -> None:
        b = self.get_repo(binary_repo)
        s = self.get_repo(source_repo)
        if not b:
            raise KeyError(f"binary repo '{binary_repo}' not found")
        if not s:
            raise KeyError(f"source repo '{source_repo}' not found")
        if b["type"] != "binary":
            raise ValueError(f"repo '{binary_repo}' is not a binary repo")
        if s["type"] != "source":
            raise ValueError(f"repo '{source_repo}' is not a source repo")
        with self.conn:
            self.conn.execute("UPDATE repositories SET source_repo_id=? WHERE id=?", (s["id"], b["id"]))

    def update_repo_timestamp(self, repo_id: int, ts: str) -> None:
        with self.conn:
            self.conn.execute("UPDATE repositories SET last_updated=? WHERE id=?", (ts, repo_id))

    # -------------------------
    # Package write helpers
    # -------------------------
    def wipe_repo_packages(self, repo_id: int) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM packages WHERE repo_id=?", (repo_id,))

    def insert_package(self, repo_id: int, pkg: Dict[str, Any]) -> int:
        """
        Insert a single package into packages. pkg is a dict of columns (excluding pkgKey).
        Returns pkgKey (lastrowid).
        """
        data = dict(pkg)
        data["repo_id"] = repo_id
        cols = list(data.keys())
        placeholders = ", ".join("?" for _ in cols)
        sql = f"INSERT INTO packages ({','.join(cols)}) VALUES ({placeholders})"
        with self.conn:
            cur = self.conn.execute(sql, tuple(data[c] for c in cols))
        return int(cur.lastrowid)

    def insert_relations(self, table: str, pkgKey: int, items: Iterable[Dict[str, Any]]) -> None:
        items = list(items)
        if not items:
            return
        base_cols = ["name", "flags", "epoch", "version", "release"]
        cols = base_cols + (["pre"] if table == "requires" else [])
        col_list = ", ".join(cols + ["pkgKey"])
        placeholders = ", ".join("?" for _ in (cols + ["pkgKey"]))
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        data = []
        for it in items:
            row = tuple(it.get(c) for c in cols) + (pkgKey,)
            data.append(row)
        with self.conn:
            self.conn.executemany(sql, data)

    def insert_filelists(self, pkgKey: int, rows: Iterable[Dict[str, Any]]) -> None:
        data = [(pkgKey, r.get("dirname"), r.get("filenames"), r.get("filetypes")) for r in rows]
        if not data:
            return
        sql = "INSERT INTO filelist (pkgKey, dirname, filenames, filetypes) VALUES (?, ?, ?, ?)"
        with self.conn:
            self.conn.executemany(sql, data)

    def insert_changelogs(self, pkgKey: int, rows: Iterable[Dict[str, Any]]) -> None:
        data = [(pkgKey, r.get("author"), r.get("date"), r.get("changelog")) for r in rows]
        if not data:
            return
        sql = "INSERT INTO changelog (pkgKey, author, date, changelog) VALUES (?, ?, ?, ?)"
        with self.conn:
            self.conn.executemany(sql, data)

    # -------------------------
    # Import repodata sqlite file (on disk)
    # -------------------------
    def import_repodb(self, sqlite_path: Union[str, Path], target_repo_name: str) -> int:
        """
        Attach an external repodata sqlite file and copy its contents into our unified DB.

        sqlite_path: filesystem path (metadata_manager writes a temp file from bytes and passes the path)
        target_repo_name: repository name (must exist)
        Returns repository id.
        """
        src_path = Path(sqlite_path)
        if not src_path.exists():
            raise FileNotFoundError(src_path)

        repo = self.get_repo(target_repo_name)
        if repo is None:
            raise KeyError(f"Target repository '{target_repo_name}' not found; create it with add_repo() first")
        repo_id = int(repo["id"])

        attach_alias = f"src_{uuid.uuid4().hex}"
        cur = self.conn.cursor()
        try:
            cur.execute(f"ATTACH DATABASE ? AS {attach_alias}", (str(src_path),))
        except sqlite3.DatabaseError as e:
            cur.close()
            raise RuntimeError(f"Failed to attach {src_path}: {e}")

        try:
            mapping: Dict[int, int] = {}

            # copy packages
            if self._table_exists_in_attached(attach_alias, "packages"):
                src_rows = list(self.conn.execute(f"SELECT * FROM {attach_alias}.packages"))
                for src in src_rows:
                    s = dict(src)
                    old_key = s.pop("pkgKey", None)
                    s.pop("repo_id", None)
                    new_key = self.insert_package(repo_id, s)
                    if old_key is not None:
                        mapping[int(old_key)] = int(new_key)

            # helper to copy relation-like tables
            def _copy_table(table_name: str, columns: List[str]) -> None:
                if not self._table_exists_in_attached(attach_alias, table_name):
                    return
                src_rows = list(self.conn.execute(f"SELECT * FROM {attach_alias}.{table_name}"))
                data = []
                for r in src_rows:
                    rd = dict(r)
                    old = rd.get("pkgKey")
                    if old is None:
                        continue
                    new = mapping.get(int(old))
                    if new is None:
                        continue
                    values = [rd.get(c) for c in columns] + [new]
                    data.append(tuple(values))
                if data:
                    col_list = ", ".join(columns + ["pkgKey"])
                    placeholders = ", ".join("?" for _ in (columns + ["pkgKey"]))
                    with self.conn:
                        self.conn.executemany(f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})", data)

            relation_cols = ["name", "flags", "epoch", "version", "release"]
            _copy_table("provides", relation_cols)
            _copy_table("requires", relation_cols + ["pre"])
            _copy_table("conflicts", relation_cols)
            _copy_table("obsoletes", relation_cols)
            _copy_table("suggests", relation_cols)
            _copy_table("enhances", relation_cols)
            _copy_table("recommends", relation_cols)
            _copy_table("supplements", relation_cols)

            # files
            if self._table_exists_in_attached(attach_alias, "files"):
                rows = list(self.conn.execute(f"SELECT name, type, pkgKey FROM {attach_alias}.files"))
                data = []
                for r in rows:
                    rd = dict(r)
                    new = mapping.get(int(rd["pkgKey"]))
                    if new is None:
                        continue
                    data.append((rd["name"], rd["type"], new))
                if data:
                    with self.conn:
                        self.conn.executemany("INSERT INTO files (name, type, pkgKey) VALUES (?, ?, ?)", data)

            # filelist
            if self._table_exists_in_attached(attach_alias, "filelist"):
                rows = list(
                    self.conn.execute(f"SELECT dirname, filenames, filetypes, pkgKey FROM {attach_alias}.filelist")
                )
                data = []
                for r in rows:
                    rd = dict(r)
                    new = mapping.get(int(rd["pkgKey"]))
                    if new is None:
                        continue
                    data.append((new, rd.get("dirname"), rd.get("filenames"), rd.get("filetypes")))
                if data:
                    with self.conn:
                        self.conn.executemany(
                            "INSERT INTO filelist (pkgKey, dirname, filenames, filetypes) VALUES (?, ?, ?, ?)", data
                        )

            # changelog
            if self._table_exists_in_attached(attach_alias, "changelog"):
                rows = list(self.conn.execute(f"SELECT pkgKey, author, date, changelog FROM {attach_alias}.changelog"))
                data = []
                for r in rows:
                    rd = dict(r)
                    new = mapping.get(int(rd["pkgKey"]))
                    if new is None:
                        continue
                    data.append((new, rd.get("author"), rd.get("date"), rd.get("changelog")))
                if data:
                    with self.conn:
                        self.conn.executemany(
                            "INSERT INTO changelog (pkgKey, author, date, changelog) VALUES (?, ?, ?, ?)", data
                        )
        finally:
            try:
                cur.execute(f"DETACH DATABASE {attach_alias}")
            except sqlite3.DatabaseError:
                _logger.debug("Detach failed for %s (ignored)", attach_alias)
            cur.close()

        return repo_id

    def _table_exists_in_attached(self, attach_alias: str, table: str) -> bool:
        q = f"SELECT name FROM {attach_alias}.sqlite_master WHERE type='table' AND name=?"
        r = self.conn.execute(q, (table,)).fetchone()
        return bool(r)

    # -------------------------
    # Query helpers (NEVRA-aware)
    # -------------------------
    def get_all_packages(self) -> Dict[int, Dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM packages").fetchall()
        return {int(r["pkgKey"]): dict(r) for r in rows}

    def get_source_repo(self, binary_repo_id: int) -> Optional[Dict[str, Any]]:
        r = self.conn.execute("SELECT source_repo_id FROM repositories WHERE id=?", (binary_repo_id,)).fetchone()
        if not r:
            return None
        src_id = r["source_repo_id"]
        if src_id is None:
            return None
        return self.get_repo(int(src_id))

    def provides_map(self) -> Dict[str, Set[int]]:
        out: Dict[str, Set[int]] = {}
        for r in self.conn.execute("SELECT name, pkgKey FROM provides"):
            out.setdefault(r["name"], set()).add(r["pkgKey"])
        return out

    def requires_map(self) -> Dict[int, List[Dict[str, Any]]]:
        out: Dict[int, List[Dict[str, Any]]] = {}
        for r in self.conn.execute("SELECT * FROM requires"):
            out.setdefault(r["pkgKey"], []).append(dict(r))
        return out

    def get_by_key(self, pkgKey: int) -> Optional[Dict[str, Any]]:
        r = self.conn.execute("SELECT * FROM packages WHERE pkgKey=?", (pkgKey,)).fetchone()
        return dict(r) if r else None

    def search_packages(self, pattern: str, repo_filter: Optional[Sequence[int]] = None) -> List[Dict[str, Any]]:
        """
        Search packages by substring matching like DNF:
        - Search in package name and summary with case-insensitive substring match.
        - Support NEVRA-string parsing for exact matching on name, epoch, version, release, arch.
        - Repo filtering supported.
        """

        self._print_repo_info(repo_filter)

        try:
            nv = NEVRA.parse(pattern)
        except Exception:
            nv = None

        params: List[Any] = []
        where_clauses: List[str] = []

        if repo_filter:
            where_clauses.append("repo_id IN ({})".format(",".join("?" for _ in repo_filter)))
            params.extend(repo_filter)

        if nv:
            # Exact matches on NEVRA fields
            where_clauses.append("name = ?")
            params.append(nv.name)
            if nv.epoch is not None:
                where_clauses.append("epoch = ?")
                params.append(nv.epoch)
            if nv.version is not None:
                where_clauses.append("version = ?")
                params.append(nv.version)
            if nv.release is not None:
                where_clauses.append("release = ?")
                params.append(nv.release)
            if nv.arch is not None:
                where_clauses.append("arch = ?")
                params.append(nv.arch)
        else:
            # Substring match on name OR summary with case-insensitive LIKE
            substr_pattern = f"%{pattern}%"
            where_clauses.append("(LOWER(name) LIKE LOWER(?) OR LOWER(summary) LIKE LOWER(?))")
            params.extend([substr_pattern, substr_pattern])

        query = "SELECT * FROM packages"
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        rows = self.conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def _print_repo_info(self, repo_ids: Optional[Sequence[int]] = None) -> None:
        # Fetch repository name(s) and last_updated times, print info like:
        # "Repository: <repo name> last metadata updated at <time>"
        # Support multiple repo ids by printing each line.
        if repo_ids is None:
            # If None, print info for all repos
            repos = self.conn.execute("SELECT name, last_updated FROM repositories ORDER BY name").fetchall()
        else:
            q = "SELECT name, last_updated FROM repositories WHERE id IN ({})".format(",".join("?" for _ in repo_ids))
            repos = self.conn.execute(q, tuple(repo_ids)).fetchall()

        for r in repos:
            name = r["name"]
            last_upd = r["last_updated"] or "never"
            print(f"Repository: {name} last metadata updated at {last_upd}")
