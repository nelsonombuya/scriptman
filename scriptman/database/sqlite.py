"""💾 SQLite database client — local file storage made simple."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import Any

from scriptman._internal import log
from scriptman.database.client import DatabaseClient
from scriptman.database.exceptions import DatabaseError


class SQLiteClient(DatabaseClient):
    """💾 SQLite database client for local file storage.

    Simple, thread-safe SQLite operations with named parameter support.
    Perfect for local data storage, caching, event logs, etc.

    Note:
        WAL Mode is enabled automatically on connect for better concurrency.
        Use `disable_wal_mode()` if you prefer traditional rollback journal.
        Use `is_wal_mode()` to check current mode.

    Args:
        path: Path to SQLite database file (created if doesn't exist)
            - Use ":memory:" for in-memory database
            - Use None for default location (config's data.db directory)
        name: Database name (used with default path). Default: "default.db"
        timeout: Connection timeout in seconds (default: 30)

    Example:
        >>> # Default location: .data/db/default.db
        >>> db = SQLiteClient()

        >>> # Custom name in default location: .data/db/myapp.db
        >>> db = SQLiteClient(name="myapp.db")

        >>> # Explicit path (ignores config)
        >>> db = SQLiteClient("path/to/custom.db")

        >>> # In-memory database
        >>> db = SQLiteClient(":memory:")

        >>> db.execute("CREATE TABLE IF NOT EXISTS events (id TEXT, message TEXT)")
        >>> db.execute(
        ...     "INSERT INTO events VALUES (:id, :msg)",
        ...     {"id": "1", "msg": "Hello"},
        ... )
        >>> events = db.query("SELECT * FROM events")
        >>> db.close()

    Context Manager:
        >>> with SQLiteClient("app.db") as db:
        ...     events = db.query("SELECT * FROM events")

    Thread Safety:
        All operations are thread-safe via internal locking.
        For high-concurrency workloads, enable WAL mode:
        >>> db.enable_wal_mode()

    Named Parameters:
        Use :name syntax for parameters:
        >>> db.query("SELECT * FROM users WHERE id = :id", {"id": 123})
    """

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        name: str = "default.db",
        timeout: float = 30.0,
    ) -> None:
        """🚀 Initialize SQLite client.

        Args:
            path: Database file path, ":memory:" for in-memory, or None for default
            name: Database filename when using default path (default: "default.db")
            timeout: Connection timeout in seconds
        """
        super().__init__()

        self._path: str | Path = self._resolve_path(path, name)
        self._timeout = timeout
        self._conn: sqlite3.Connection | None = None
        self._lock = RLock()  # Reentrant lock for nested execute() in transaction()
        self._in_transaction = False  # Track if we're inside a transaction

    def _resolve_path(self, path: str | Path | None, name: str) -> str | Path:
        """📁 Resolve database path based on input.

        Resolution order:
            1. ":memory:" → in-memory database
            2. Explicit path → use as-is (create parent dirs)
            3. None → use config's data.db directory + name

        Args:
            path: User-provided path (or None)
            name: Database filename for default location

        Returns:
            Resolved path string or Path object
        """
        # In-memory database
        if path == ":memory:":
            return path

        # Explicit path provided
        if path is not None:
            resolved = Path(path)
            resolved.parent.mkdir(parents=True, exist_ok=True)
            return resolved

        # Default: use config's db directory
        from scriptman import config

        db_dir = config.ensure_path("db")
        return db_dir / name

    # ─────────────────────────────────────────────────────────────
    # Properties
    # ─────────────────────────────────────────────────────────────

    @property
    def database_type(self) -> str:
        """🏷️ Database type identifier."""
        return "sqlite"

    @property
    def is_connected(self) -> bool:
        """🔌 Check if connection is active."""
        return self._conn is not None

    @property
    def path(self) -> str:
        """📁 Database file path."""
        return str(self._path)

    # ─────────────────────────────────────────────────────────────
    # Connection Management
    # ─────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """🔌 Establish SQLite connection."""
        if self._conn is not None:
            return  # Already connected

        try:
            self._conn = sqlite3.connect(
                str(self._path),
                timeout=self._timeout,
                check_same_thread=False,  # We handle thread safety with lock
            )
            self._conn.row_factory = sqlite3.Row  # Dict-like row access
            self.enable_wal_mode()
            log.debug(f"🔌 Connected to SQLite: {self._path} with WAL mode")
        except sqlite3.Error as e:
            log.critical(f"🔥 Failed to connect to database: {self._path}")
            raise DatabaseError(f"Failed to connect to {self._path}", e)

    def close(self) -> None:
        """🔌 Close SQLite connection."""
        if self._conn is None:
            return  # Already closed

        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
                log.debug(f"🔌 Closed SQLite: {self._path}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """🔒 Transaction context manager."""
        self._ensure_connected()

        if self._conn is None:
            raise DatabaseError("Connection is not established")

        with self._lock:
            self._in_transaction = True
            try:
                yield
                self._conn.commit()
            except sqlite3.Error as e:
                # Wrap database errors in DatabaseError
                self._conn.rollback()
                raise DatabaseError("Transaction failed", e)
            except Exception:
                # Re-raise user exceptions unchanged (don't hide their type)
                self._conn.rollback()
                raise
            finally:
                self._in_transaction = False

    # ─────────────────────────────────────────────────────────────
    # Query Execution
    # ─────────────────────────────────────────────────────────────

    def query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """🔍 Execute SELECT query and return results."""
        self._ensure_connected()

        # Convert canonical :name syntax to native format (pass-through for SQLite)
        native_sql, native_params = self._prepare_query(sql, params)

        with self._lock:
            try:
                if self._conn is None:
                    raise DatabaseError("Connection is not established")

                cursor = self._conn.execute(native_sql, native_params or {})
                return [dict(row) for row in cursor.fetchall()]
            except sqlite3.Error as e:
                log.error(f"❌ Query failed: {sql[:100]}...")
                raise DatabaseError("Query execution failed", e)

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """✍️ Execute INSERT/UPDATE/DELETE and return affected rows."""
        self._ensure_connected()

        # Convert canonical :name syntax to native format (pass-through for SQLite)
        native_sql, native_params = self._prepare_query(sql, params)

        with self._lock:
            try:
                if self._conn is None:
                    raise DatabaseError("Connection is not established")

                cursor = self._conn.execute(native_sql, native_params or {})
                # Only auto-commit if NOT inside a transaction
                if not self._in_transaction:
                    self._conn.commit()
                return cursor.rowcount
            except sqlite3.Error as e:
                if self._conn is not None and not self._in_transaction:
                    self._conn.rollback()
                log.error(f"❌ Execute failed: {sql[:100]}...")
                raise DatabaseError("Execute failed", e)

    def execute_many(
        self,
        sql: str,
        params_list: list[dict[str, Any]],
    ) -> int:
        """✍️ Execute bulk INSERT/UPDATE/DELETE."""
        if not params_list:
            return 0

        self._ensure_connected()

        # For execute_many, we only need to convert the SQL once
        # (SQLite's executemany accepts the same SQL with multiple param dicts)
        native_sql, _ = self._prepare_query(sql, None)

        with self._lock:
            try:
                if self._conn is None:
                    raise DatabaseError("Connection is not established")

                cursor = self._conn.executemany(native_sql, params_list)
                # Only auto-commit if NOT inside a transaction
                if not self._in_transaction:
                    self._conn.commit()
                return cursor.rowcount
            except sqlite3.Error as e:
                if self._conn is not None and not self._in_transaction:
                    self._conn.rollback()
                log.error(f"❌ Execute many failed: {sql[:100]}...")
                raise DatabaseError("Bulk execute failed", e)

    def execute_script(self, sql: str) -> None:
        """📜 Execute multiple SQL statements (DDL scripts).

        Use for schema creation with multiple statements.

        Args:
            sql: SQL script with multiple statements

        Raises:
            DatabaseError: If script execution fails

        Example:
            >>> db.execute_script('''
            ...     CREATE TABLE users (id INTEGER, name TEXT);
            ...     CREATE TABLE orders (id INTEGER, user_id INTEGER);
            ...     CREATE INDEX idx_orders_user ON orders(user_id);
            ... ''')
        """
        self._ensure_connected()

        with self._lock:
            try:
                if self._conn is None:
                    raise DatabaseError("Connection is not established")

                self._conn.executescript(sql)
                # Only auto-commit if NOT inside a transaction
                if not self._in_transaction:
                    self._conn.commit()
            except sqlite3.Error as e:
                if self._conn is not None and not self._in_transaction:
                    self._conn.rollback()
                log.exception(f"❌ Script execution failed: {sql[:100]}...")
                raise DatabaseError("Script execution failed", e)

    # ─────────────────────────────────────────────────────────────
    # SQLite-Specific Methods
    # ─────────────────────────────────────────────────────────────

    def table_exists(self, table_name: str) -> bool:
        """❓ Check if table exists (SQLite-specific query)."""
        sql = """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = :table_name
        """
        return bool(self.query(sql, {"table_name": table_name}))

    def create_table(
        self,
        table_name: str,
        columns: dict[str, str],
        primary_key: list[str] | None = None,
        if_not_exists: bool = True,
    ) -> None:
        """🏗️ Create a table.

        Args:
            table_name: Name of the table
            columns: Column definitions as {name: type}
            primary_key: Column names for composite primary key
            if_not_exists: Add IF NOT EXISTS clause

        Raises:
            DatabaseError: If table creation fails

        Example:
            >>> db.create_table(
            ...     "events",
            ...     {
            ...         "id": "TEXT",
            ...         "timestamp": "TEXT NOT NULL",
            ...         "message": "TEXT",
            ...         "data": "TEXT",
            ...     },
            ...     primary_key=["id"]
            ... )
        """
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        col_defs = ", ".join(f'"{name}" {dtype}' for name, dtype in columns.items())

        if primary_key:
            pk_cols = ", ".join(f'"{col}"' for col in primary_key)
            col_defs += f", PRIMARY KEY ({pk_cols})"

        sql = f'CREATE TABLE {exists_clause}"{table_name}" ({col_defs})'
        self.execute(sql)
        log.debug(f"🏗️ Created table: {table_name}")

    def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """🗑️ Drop a table.

        Args:
            table_name: Name of the table
            if_exists: Add IF EXISTS clause

        Raises:
            DatabaseError: If table drop fails
        """
        exists_clause = "IF EXISTS " if if_exists else ""
        self.execute(f'DROP TABLE {exists_clause}"{table_name}"')
        log.debug(f"🗑️ Dropped table: {table_name}")

    def create_index(
        self,
        index_name: str,
        table_name: str,
        columns: list[str],
        unique: bool = False,
        if_not_exists: bool = True,
    ) -> None:
        """📇 Create an index.

        Args:
            index_name: Name for the index
            table_name: Table to index
            columns: Columns to include in index
            unique: Create unique index
            if_not_exists: Add IF NOT EXISTS clause

        Raises:
            DatabaseError: If index creation fails

        Example:
            >>> db.create_index(
            ...     "idx_events_timestamp",
            ...     "events",
            ...     ["timestamp"]
            ... )
        """
        unique_clause = "UNIQUE " if unique else ""
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        cols = ", ".join(f'"{col}"' for col in columns)

        sql = (
            f'CREATE {unique_clause}INDEX {exists_clause}"{index_name}" '
            f'ON "{table_name}" ({cols})'
        )
        self.execute(sql)
        log.debug(f"📇 Created index: {index_name}")

    def list_tables(self) -> list[str]:
        """📋 List all tables in the database.

        Returns:
            List of table names
        """
        rows = self.query(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        )
        return [row["name"] for row in rows]

    def vacuum(self) -> None:
        """🧹 Optimize database file (reclaim space).

        Rebuilds the database file, reclaiming unused space.
        Run periodically after many deletes.

        Raises:
            DatabaseError: If vacuum operation fails
        """
        self.execute("VACUUM")
        log.debug("🧹 Vacuumed database")

    def enable_wal_mode(self) -> None:
        """⚡ Enable WAL mode for better concurrent performance.

        Write-Ahead Logging allows concurrent reads during writes.
        Recommended for high-concurrency workloads.
        Called automatically on connect().
        """
        self.execute("PRAGMA journal_mode = WAL")
        log.debug("⚡ Enabled WAL mode")

    def disable_wal_mode(self) -> None:
        """🔒 Disable WAL mode and use traditional rollback journal.

        Traditional mode may have better single-writer performance
        but doesn't allow concurrent reads during writes.
        Use for low-concurrency or single-threaded workloads.
        """
        self.execute("PRAGMA journal_mode = DELETE")
        log.debug("🔒 Disabled WAL mode")

    def is_wal_mode(self) -> bool:
        """🔍 Check if WAL mode is currently enabled."""
        return str(self.get_pragma("journal_mode")).lower() == "wal"

    def get_pragma(self, pragma: str) -> Any:
        """⚙️ Get a SQLite pragma value.

        Args:
            pragma: Pragma name (e.g., 'journal_mode', 'foreign_keys')

        Returns:
            Pragma value
        """
        return self.query_value(f"PRAGMA {pragma}")

    def set_pragma(self, pragma: str, value: Any) -> None:
        """⚙️ Set a SQLite pragma value.

        Args:
            pragma: Pragma name
            value: Value to set
        """
        self.execute(f"PRAGMA {pragma} = {value}")

    # ─────────────────────────────────────────────────────────────
    # Internal Helpers
    # ─────────────────────────────────────────────────────────────

    def _ensure_connected(self) -> None:
        """🔌 Ensure connection is active (lazy connect)."""
        if self._conn is None:
            self.connect()


__all__ = ["SQLiteClient"]
