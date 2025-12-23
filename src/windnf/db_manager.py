from __future__ import annotations

import sqlite3
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
    Optimized for batch operations and Thread-Safe imports.
    """

    def __init__(self, config: Config, schema_path: Optional[Union[str, Path]] = None):
        self.config = config
        self.db_path = Path(self.config.db_path)
        
        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Main connection for READS and lightweight writes (non-threaded)
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._configure_pragmas(self.conn)

        # Load schema
        schema_file = Path(schema_path) if schema_path else (Path(__file__).parent / "schema.sql")
        if schema_file.exists():
            with open(schema_file, "r", encoding="utf-8") as fh:
                self.conn.executescript(fh.read())
        else:
            _logger.debug("Schema file not found at %s — assuming DB already initialized.", schema_file)

    # -------------------------
    # PRAGMA tuning
    # -------------------------
    def _configure_pragmas(self, connection: sqlite3.Connection) -> None:
        """Apply performance/integrity pragmas to ANY connection."""
        pragmas = (
            "PRAGMA foreign_keys=ON;",  # Default to ON
            "PRAGMA synchronous=NORMAL;",
            "PRAGMA journal_mode=WAL;",
            "PRAGMA cache_size=-64000;",  # 64MB RAM
            "PRAGMA temp_store=MEMORY;",
        )
        try:
            cur = connection.cursor()
            for p in pragmas:
                cur.execute(p)
            cur.close()
        except Exception as e:
            _logger.debug("PRAGMA configuration failed: %s", e)

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
        if not b or not s:
            raise KeyError("Repo not found")
        
        with self.conn:
            self.conn.execute("UPDATE repositories SET source_repo_id=? WHERE id=?", (s["id"], b["id"]))

    def update_repo_timestamp(self, repo_id: int, ts: str) -> None:
        with self.conn:
            self.conn.execute("UPDATE repositories SET last_updated=? WHERE id=?", (ts, repo_id))

    # -------------------------
    # Package Write Helpers
    # -------------------------
    def wipe_repo_packages(self, repo_id: int) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM packages WHERE repo_id=?", (repo_id,))

    def insert_package(self, repo_id: int, pkg: Dict[str, Any], cursor: Optional[sqlite3.Cursor] = None) -> int:
        data = dict(pkg)
        data["repo_id"] = repo_id
        cols = list(data.keys())
        placeholders = ", ".join("?" for _ in cols)
        sql = f"INSERT INTO packages ({','.join(cols)}) VALUES ({placeholders})"
        values = tuple(data[c] for c in cols)

        if cursor:
            cursor.execute(sql, values)
            return int(cursor.lastrowid)
        else:
            with self.conn:
                cur = self.conn.execute(sql, values)
                return int(cur.lastrowid)

    # -------------------------
    # Import Logic (Thread-Safe & FK-Safe Patch)
    # -------------------------
    def import_repodb(self, sqlite_path: Union[str, Path], target_repo_name: str) -> int:
        """
        Thread-Safe Streaming Import.
        CRITICAL: Disables Foreign Keys on the local connection to prevent 
        spurious IntegrityErrors during bulk insert.
        """
        src_path = Path(sqlite_path)
        if not src_path.exists():
            raise FileNotFoundError(src_path)

        repo = self.get_repo(target_repo_name)
        if repo is None:
            raise KeyError(f"Target repository '{target_repo_name}' not found")
        repo_id = int(repo["id"])

        # Create PRIVATE connection for this thread
        local_conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        local_conn.row_factory = sqlite3.Row
        self._configure_pragmas(local_conn)
        
        # --- THE FIX: Disable FK Checks for Import Session ---
        local_conn.execute("PRAGMA foreign_keys=OFF;") 

        attach_alias = f"src_{uuid.uuid4().hex}"
        cur = local_conn.cursor()
        
        try:
            cur.execute(f"ATTACH DATABASE ? AS {attach_alias}", (str(src_path),))
            local_conn.execute("BEGIN TRANSACTION")
            
            try:
                mapping: Dict[int, int] = {}
                
                # Copy Packages
                if self._table_exists_in_attached(attach_alias, "packages", cur):
                    cur.execute(f"SELECT * FROM {attach_alias}.packages")
                    insert_cur = local_conn.cursor()
                    
                    while True:
                        src = cur.fetchone()
                        if not src: break
                        s = dict(src)
                        old_key = s.pop("pkgKey", None)
                        s.pop("repo_id", None)
                        
                        new_key = self.insert_package(repo_id, s, cursor=insert_cur)
                        if old_key is not None:
                            mapping[int(old_key)] = int(new_key)
                    insert_cur.close()

                # Copy Tables
                def _copy_table(table_name: str, columns: List[str]):
                    if not self._table_exists_in_attached(attach_alias, table_name, cur): return
                    
                    read_cur = local_conn.cursor()
                    read_cur.execute(f"SELECT * FROM {attach_alias}.{table_name}")
                    write_cur = local_conn.cursor()
                    
                    col_list = ", ".join(columns + ["pkgKey"])
                    placeholders = ", ".join("?" for _ in (columns + ["pkgKey"]))
                    sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
                    
                    batch = []
                    BATCH_SIZE = 5000
                    for r in read_cur:
                        rd = dict(r)
                        old = rd.get("pkgKey")
                        if old is None: continue
                        new = mapping.get(int(old))
                        if new is None: continue
                        
                        values = [rd.get(c) for c in columns] + [new]
                        batch.append(tuple(values))
                        if len(batch) >= BATCH_SIZE:
                            write_cur.executemany(sql, batch)
                            batch = []
                    if batch:
                        write_cur.executemany(sql, batch)
                    read_cur.close()
                    write_cur.close()

                # Execute Copies
                relation_cols = ["name", "flags", "epoch", "version", "release"]
                _copy_table("provides", relation_cols)
                _copy_table("requires", relation_cols + ["pre"])
                _copy_table("conflicts", relation_cols)
                _copy_table("obsoletes", relation_cols)
                _copy_table("suggests", relation_cols)
                _copy_table("enhances", relation_cols)
                _copy_table("recommends", relation_cols)
                _copy_table("supplements", relation_cols)
                
                # Copy Files
                if self._table_exists_in_attached(attach_alias, "files", cur):
                     read_cur = local_conn.cursor()
                     read_cur.execute(f"SELECT name, type, pkgKey FROM {attach_alias}.files")
                     write_cur = local_conn.cursor()
                     batch = []
                     for r in read_cur:
                         rd = dict(r)
                         new = mapping.get(int(rd["pkgKey"]))
                         if new:
                             batch.append((rd["name"], rd["type"], new))
                             if len(batch) >= 5000:
                                 write_cur.executemany("INSERT INTO files (name, type, pkgKey) VALUES (?, ?, ?)", batch)
                                 batch = []
                     if batch:
                         write_cur.executemany("INSERT INTO files (name, type, pkgKey) VALUES (?, ?, ?)", batch)
                     read_cur.close()
                     write_cur.close()

                local_conn.commit()
                
            except Exception:
                local_conn.rollback()
                raise

        finally:
            try:
                local_conn.close()
            except Exception:
                pass

        return repo_id

    def _table_exists_in_attached(self, attach_alias: str, table: str, cursor: sqlite3.Cursor) -> bool:
        try:
            q = f"SELECT name FROM {attach_alias}.sqlite_master WHERE type='table' AND name=?"
            r = cursor.execute(q, (table,)).fetchone()
            return bool(r)
        except Exception:
            return False

    # -------------------------
    # Query Helpers
    # -------------------------
    def get_by_key(self, pkgKey: int) -> Optional[Dict[str, Any]]:
        r = self.conn.execute("SELECT * FROM packages WHERE pkgKey=?", (pkgKey,)).fetchone()
        return dict(r) if r else None

    def search_packages(self, pattern: str, repo_filter: Optional[Sequence[int]] = None) -> List[Dict[str, Any]]:
        has_star = "*" in pattern
        try:
            nv = NEVRA.parse(pattern) if not has_star else None
        except Exception:
            nv = None

        params = []
        where = []
        if repo_filter:
            where.append(f"repo_id IN ({','.join('?' for _ in repo_filter)})")
            params.extend(repo_filter)

        if nv:
            where.append("name = ?")
            params.append(nv.name)
            if nv.version:
                where.append("version = ?")
                params.append(nv.version)
            if nv.release:
                where.append("release = ?")
                params.append(nv.release)
            if nv.arch:
                where.append("arch = ?")
                params.append(nv.arch)
        else:
            sql_pattern = pattern.replace("*", "%")
            if not has_star:
                sql_pattern = f"%{pattern}%"
            where.append("(LOWER(name) LIKE LOWER(?) OR LOWER(summary) LIKE LOWER(?))")
            params.extend([sql_pattern, sql_pattern])

        query = "SELECT * FROM packages"
        if where:
            query += " WHERE " + " AND ".join(where)

        rows = self.conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]
    
    def provides_map(self) -> Dict[str, Set[int]]:
        """
        Builds a map of 'capability_name' -> {set of pkgKeys}.
        CRITICAL FIX: Includes both explicit 'provides' entries AND 
        the actual package names themselves (Implicit Provides).
        """
        out: Dict[str, Set[int]] = {}
        
        # 1. Explicit Provides (from 'provides' table)
        cursor = self.conn.execute("SELECT name, pkgKey FROM provides")
        for r in cursor:
            out.setdefault(r["name"], set()).add(r["pkgKey"])
            
        # 2. Implicit Provides (from 'packages' table)
        cursor = self.conn.execute("SELECT name, pkgKey FROM packages")
        for r in cursor:
            out.setdefault(r["name"], set()).add(r["pkgKey"])
            
        return out

    def requires_map(self) -> Dict[int, List[Dict[str, Any]]]:
        out: Dict[int, List[Dict[str, Any]]] = {}
        for r in self.conn.execute("SELECT * FROM requires"):
            out.setdefault(r["pkgKey"], []).append(dict(r))
        return out