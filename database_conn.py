from __future__ import annotations
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterable, List, Optional, Sequence, Tuple, Dict, Union

try:
    import pandas as pd  # optional
except Exception:
    pd = None


class SQLiteClient:
    """
    Simple OOP helper for SQLite databases (perfect for USB paths like D:\\nba.db).

    Features:
    - Safe parameterized queries (avoid SQL injection)
    - Auto-connect/close, context-managed transactions
    - Execute (DDL/DML) and query (SELECT) helpers
    - Executemany for bulk inserts
    - Convenience create_table from a schema dict
    - Optional pandas DataFrame helpers (if pandas installed)
    """

    def __init__(self, db_path: str, timeout: float = 30.0, pragmas: Optional[Dict[str, Any]] = None):
        """
        Args:
            db_path: Path to .db file (e.g., r"D:\\nba.db")
            timeout: SQLite lock timeout in seconds
            pragmas: Optional PRAGMAs to set after connect, e.g. {"journal_mode": "WAL", "synchronous": "NORMAL"}
        """
        self.db_path = db_path
        self.timeout = timeout
        self._conn: Optional[sqlite3.Connection] = None
        self._pragmas = pragmas or {
            "journal_mode": "WAL",
            "synchronous": "NORMAL",
            "foreign_keys": 1
        }

    # ---------- Connection management ----------
    def connect(self) -> None:
        if self._conn is None:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            conn.row_factory = sqlite3.Row  # enables dict-like access
            self._conn = conn
            self._apply_pragmas()

    def _apply_pragmas(self) -> None:
        if not self._conn:
            return
        cur = self._conn.cursor()
        for k, v in self._pragmas.items():
            cur.execute(f"PRAGMA {k}={v}")
        cur.close()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "SQLiteClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # On error, rollback current transaction; else commit any pending changes
        if self._conn is not None:
            if exc_type is not None:
                self._conn.rollback()
            else:
                self._conn.commit()
        self.close()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self.connect()
        assert self._conn is not None
        return self._conn

    # ---------- Transaction helper ----------
    @contextmanager
    def transaction(self):
        """Explicit transaction block with commit/rollback."""
        try:
            yield
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    # ---------- Core methods ----------
    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        """Run a DDL/DML statement. Commits immediately."""
        cur = self.conn.cursor()
        cur.execute(sql, params or [])
        self.conn.commit()
        cur.close()

    def executemany(self, sql: str, param_list: Iterable[Sequence[Any]]) -> None:
        """Run a parameterized statement for many rows (bulk insert/update)."""
        cur = self.conn.cursor()
        cur.executemany(sql, param_list)
        self.conn.commit()
        cur.close()

    def query(
        self, sql: str, params: Sequence[Any] | None = None
    ) -> List[Dict[str, Any]]:
        """Run a SELECT and return list of dict rows."""
        cur = self.conn.cursor()
        cur.execute(sql, params or [])
        rows = [dict(row) for row in cur.fetchall()]
        cur.close()
        return rows

    def query_one(
        self, sql: str, params: Sequence[Any] | None = None
    ) -> Optional[Dict[str, Any]]:
        """Return first row as dict (or None)."""
        cur = self.conn.cursor()
        cur.execute(sql, params or [])
        row = cur.fetchone()
        cur.close()
        return dict(row) if row else None

    # ---------- Schema & upserts ----------
    def create_table(self, name: str, schema: Dict[str, str], if_not_exists: bool = True) -> None:
        """
        Create a table from a {column_name: SQL_type_and_constraints} mapping.
        Example:
            schema = {
                "player_id": "INTEGER PRIMARY KEY",
                "name": "TEXT NOT NULL",
                "pts": "REAL",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            }
        """
        cols = ", ".join([f"{col} {decl}" for col, decl in schema.items()])
        ine = "IF NOT EXISTS " if if_not_exists else ""
        ddl = f"CREATE TABLE {ine}{name} ({cols});"
        self.execute(ddl)

    def insert(self, table: str, row: Dict[str, Any]) -> None:
        cols = ", ".join(row.keys())
        placeholders = ", ".join(["?"] * len(row))
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        self.execute(sql, list(row.values()))

    def upsert(self, table: str, row: Dict[str, Any], conflict_cols: Union[str, Sequence[str]]) -> None:
        """
        SQLite UPSERT (requires a UNIQUE or PRIMARY KEY constraint on conflict_cols).
        conflict_cols: single column name or sequence of names.
        """
        if isinstance(conflict_cols, str):
            conflict_cols = [conflict_cols]
        cols = list(row.keys())
        placeholders = ", ".join(["?"] * len(cols))
        assignments = ", ".join([f"{c}=excluded.{c}" for c in cols if c not in conflict_cols])
        sql = (
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT({', '.join(conflict_cols)}) DO UPDATE SET {assignments};"
        )
        self.execute(sql, [row[c] for c in cols])

    # ---------- Pandas helpers ----------
    def to_dataframe(self, sql: str, params: Sequence[Any] | None = None):
        if pd is None:
            raise ImportError("pandas is not installed. `pip install pandas`")
        return pd.read_sql_query(sql, self.conn, params=params or [])

    def dataframe_to_table(
        self,
        df, table: str, if_exists: str = "append", index: bool = False,
        dtype: Optional[Dict[str, Any]] = None
    ):
        """
        Write a pandas DataFrame to a table (create/replace/append).
        if_exists in {'fail','replace','append'}
        """
        if pd is None:
            raise ImportError("pandas is not installed. `pip install pandas`")
        df.to_sql(table, self.conn, if_exists=if_exists, index=index, dtype=dtype)

### TESTING

# # Point to your USB database file
# db_path = r"D:\nba.db"

# # Create or use the DB
# with SQLiteClient(db_path) as db:
#     # 1) Create a table (only once)
#     db.create_table(
#         "players",
#         {
#             "player_id": "INTEGER PRIMARY KEY",
#             "name": "TEXT NOT NULL",
#             "team": "TEXT",
#             "pts": "REAL",
#             "ast": "REAL",
#             "trb": "REAL",
#             "UNIQUE(name, team)"  : ""  # example composite uniqueness for upserts
#         }
#     )

#     # 2) Insert rows safely
#     db.insert("players", {"player_id": 1, "name": "LeBron James", "team": "LAL", "pts": 25.7, "ast": 8.3, "trb": 7.3})
#     db.insert("players", {"player_id": 2, "name": "Nikola Jokic", "team": "DEN", "pts": 26.4, "ast": 9.0, "trb": 12.4})

#     # 3) Upsert (insert or update on conflict)
#     db.upsert("players", {"player_id": 2, "name": "Nikola Jokic", "team": "DEN", "pts": 27.1, "ast": 9.1, "trb": 12.8}, "player_id")

#     # 4) Query as list of dicts
#     rows = db.query("SELECT name, team, pts FROM players WHERE pts > ?", (26,))
#     print(rows)

#     # 5) Transaction block (multiple operations, one commit)
#     with db.transaction():
#         db.execute("UPDATE players SET pts = pts + 0.1 WHERE team = ?", ("LAL",))
#         db.execute("DELETE FROM players WHERE name = ?", ("Some Waived Guy",))

#     # 6) Pandas integration (optional)
#     if pd:
#         df = db.to_dataframe("SELECT * FROM players ORDER BY pts DESC")
#         print(df.head())

#     # 7) Clean up: drop the table after testing
#     db.execute("DROP TABLE IF EXISTS players;")
#     db.execute("DROP TABLE IF EXISTS users;")
#     print("Table 'players' has been removed.")

# # db_path = r"D:\nba.db"

# with SQLiteClient(db_path) as db:
#     # Query for all tables in the database
#     tables = db.query("SELECT name FROM sqlite_master WHERE type='table'")
#     print("Tables:", [t["name"] for t in tables])

# #     db.execute("DROP TABLE IF EXISTS players;")
# #     print("Table 'players' has been removed.")

# '''
# OUTPUT: 

# [{'name': 'Nikola Jokic', 'team': 'DEN', 'pts': 27.1}]
#    player_id          name team   pts  ast   trb
# 0          2  Nikola Jokic  DEN  27.1  9.1  12.8
# 1          1  LeBron James  LAL  25.8  8.3   7.3
# Table 'players' has been removed.
# Tables: ['last_season_conference']
# '''