"""🏢 Microsoft SQL Server client via PyODBC."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from threading import RLock
from typing import Any

from scriptman._internal import log
from scriptman.database.client import DatabaseClient, PreparedQuery
from scriptman.database.exceptions import DatabaseError

# ═══════════════════════════════════════════════════════════════════════════════
# OPTIONAL DEPENDENCY
# ═══════════════════════════════════════════════════════════════════════════════

try:
    import pyodbc
except ImportError as e:
    raise ImportError(
        "🚫 MSSQL support requires pyodbc. Install with:\n"
        "    pip install scriptman[mssql]\n"
        "    # or\n"
        "    pip install pyodbc"
    ) from e


class MSSQLClient(DatabaseClient):
    """🏢 Microsoft SQL Server client via PyODBC.

    Supports both Windows Authentication and SQL Server Authentication.
    Uses PyODBC's built-in connection pooling for performance.

    Args:
        server: Server hostname or IP (e.g., "localhost", "192.168.1.1\\SQLEXPRESS")
        database: Database name
        username: SQL Server username (for SQL auth, mutually exclusive with
            trusted_connection)
        password: SQL Server password (for SQL auth)
        trusted_connection: Use Windows authentication (default: False)
        driver: ODBC driver name (default: "{ODBC Driver 17 for SQL Server}")
        timeout: Connection timeout in seconds (default: 30)

    Example:
        >>> # Windows Authentication
        >>> db = MSSQLClient("localhost", "mydb", trusted_connection=True)

        >>> # SQL Server Authentication
        >>> db = MSSQLClient("localhost", "mydb", username="sa", password="secret")

        >>> # Named instance
        >>> db = MSSQLClient("localhost\\SQLEXPRESS", "mydb", trusted_connection=True)

        >>> # Context manager
        >>> with MSSQLClient("localhost", "mydb", trusted_connection=True) as db:
        ...     users = db.query("SELECT * FROM users")

    Query Portability:
        Use `:name` syntax for parameters — automatically converted to `?`
        placeholders:
        >>> db.query("SELECT * FROM users WHERE age > :min_age", {"min_age": 18})
    """

    def __init__(
        self,
        server: str,
        database: str,
        *,
        username: str | None = None,
        password: str | None = None,
        trusted_connection: bool = False,
        driver: str = "{ODBC Driver 17 for SQL Server}",
        timeout: int = 30,
    ) -> None:
        """🚀 Initialize MSSQL client.

        Args:
            server: Server hostname or IP
            database: Database name
            username: SQL Server username (for SQL auth)
            password: SQL Server password (for SQL auth)
            trusted_connection: Use Windows authentication
            driver: ODBC driver name
            timeout: Connection timeout in seconds

        Raises:
            ValueError: If authentication configuration is invalid
        """
        super().__init__()

        # Validate auth configuration
        if trusted_connection and (username or password):
            raise ValueError(
                "⚠️ Cannot use both trusted_connection and username/password. "
                "Choose one authentication method."
            )
        if not trusted_connection and not (username and password):
            raise ValueError(
                "⚠️ Must provide either trusted_connection=True "
                "or both username and password."
            )

        self._server = server
        self._database = database
        self._username = username
        self._password = password
        self._trusted_connection = trusted_connection
        self._driver = driver
        self._timeout = timeout
        self._conn: pyodbc.Connection | None = None
        self._lock = RLock()  # Reentrant lock for nested execute() in transaction()
        self._in_transaction = False  # Track if we're inside a transaction

    # ─────────────────────────────────────────────────────────────────────────
    # Properties
    # ─────────────────────────────────────────────────────────────────────────

    @property
    def database_type(self) -> str:
        """🏷️ Database type identifier."""
        return "mssql"

    @property
    def is_connected(self) -> bool:
        """🔌 Check if connection is active."""
        return self._conn is not None

    @property
    def server(self) -> str:
        """🖥️ Server hostname."""
        return self._server

    @property
    def database(self) -> str:
        """🗄️ Database name."""
        return self._database

    # ─────────────────────────────────────────────────────────────────────────
    # Connection Management
    # ─────────────────────────────────────────────────────────────────────────

    def _build_connection_string(self) -> str:
        """🔧 Build ODBC connection string.

        Returns:
            ODBC connection string with all configured options.
        """
        parts = [
            f"DRIVER={self._driver}",
            f"SERVER={self._server}",
            f"DATABASE={self._database}",
        ]

        if self._trusted_connection:
            parts.append("Trusted_Connection=yes")
        else:
            parts.append(f"UID={self._username}")
            parts.append(f"PWD={self._password}")

        return ";".join(parts)

    def connect(self) -> None:
        """🔌 Establish MSSQL connection.

        Raises:
            DatabaseError: If connection fails
        """
        if self._conn is not None:
            return  # Already connected

        try:
            conn_str = self._build_connection_string()
            self._conn = pyodbc.connect(conn_str, timeout=self._timeout)
            log.debug(f"🔌 Connected to MSSQL: {self._server}/{self._database}")
        except pyodbc.Error as e:
            log.critical(
                f"🔥 Failed to connect to MSSQL: {self._server}/{self._database}"
            )
            raise DatabaseError(
                f"Failed to connect to {self._server}/{self._database}", e
            ) from e

    def close(self) -> None:
        """🔌 Close MSSQL connection."""
        if self._conn is None:
            return  # Already closed

        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
                log.debug(f"🔌 Closed MSSQL: {self._server}/{self._database}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """🔒 Transaction context manager.

        Yields:
            None

        Raises:
            DatabaseError: If transaction fails
        """
        self._ensure_connected()

        if self._conn is None:
            raise DatabaseError("Connection is not established")

        with self._lock:
            self._in_transaction = True
            try:
                yield
                self._conn.commit()
            except pyodbc.Error as e:
                self._conn.rollback()
                raise DatabaseError("Transaction failed", e) from e
            except Exception as e:
                self._conn.rollback()
                raise e
            finally:
                self._in_transaction = False

    # ─────────────────────────────────────────────────────────────────────────
    # Query Translation (:name → ?)
    # ─────────────────────────────────────────────────────────────────────────

    def _prepare_query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> PreparedQuery:
        """🔄 Convert :name syntax to ? positional placeholders.

        PyODBC uses ? for positional parameters. This method:
        1. Extracts parameter names in order of appearance
        2. Replaces :name with ?
        3. Converts dict to ordered list

        Args:
            sql: SQL query with :name parameters
            params: Parameter values as dictionary

        Returns:
            Tuple of (converted_sql, ordered_params_list)

        Raises:
            DatabaseError: If a referenced parameter is missing from params dict
        """
        if params is None:
            return sql, None

        # Extract param names in order of appearance
        param_names = re.findall(r":(\w+)", sql)

        # Replace :name with ?
        converted_sql = re.sub(r":\w+", "?", sql)

        # Convert dict to ordered list matching placeholder order
        try:
            converted_params = [params[name] for name in param_names]
        except KeyError as e:
            raise DatabaseError(f"Missing parameter: {e}") from e

        return converted_sql, converted_params

    # ─────────────────────────────────────────────────────────────────────────
    # Query Execution
    # ─────────────────────────────────────────────────────────────────────────

    def query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """🔍 Execute SELECT query and return results.

        Args:
            sql: SQL query with :name parameters
            params: Parameter values as dictionary

        Returns:
            List of rows as dictionaries

        Raises:
            DatabaseError: If query fails
        """
        self._ensure_connected()

        native_sql, native_params = self._prepare_query(sql, params)

        with self._lock:
            try:
                if self._conn is None:
                    raise DatabaseError("Connection is not established")

                cursor = self._conn.cursor()
                if native_params:
                    cursor.execute(native_sql, native_params)
                else:
                    cursor.execute(native_sql)

                columns = [column[0] for column in cursor.description or []]
                return [
                    dict(zip(columns, row, strict=False)) for row in cursor.fetchall()
                ]
            except pyodbc.Error as e:
                log.error(f"❌ Query failed: {sql[:100]}...")
                raise DatabaseError("Query execution failed", e) from e

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """✍️ Execute INSERT/UPDATE/DELETE and return affected rows.

        Args:
            sql: SQL query with :name parameters
            params: Parameter values as dictionary

        Returns:
            Number of affected rows

        Raises:
            DatabaseError: If execution fails
        """
        self._ensure_connected()

        native_sql, native_params = self._prepare_query(sql, params)

        with self._lock:
            try:
                if self._conn is None:
                    raise DatabaseError("Connection is not established")

                cursor = self._conn.cursor()
                if native_params:
                    cursor.execute(native_sql, native_params)
                else:
                    cursor.execute(native_sql)
                # Only auto-commit if NOT inside a transaction
                if not self._in_transaction:
                    self._conn.commit()
                return int(cursor.rowcount)
            except pyodbc.Error as e:
                if self._conn is not None and not self._in_transaction:
                    self._conn.rollback()
                log.error(f"❌ Execute failed: {sql[:100]}...")
                raise DatabaseError("Execute failed", e) from e

    def execute_many(
        self,
        sql: str,
        params_list: list[dict[str, Any]],
    ) -> int:
        """✍️ Execute bulk INSERT/UPDATE/DELETE.

        Args:
            sql: SQL query with :name parameters
            params_list: List of parameter dictionaries

        Returns:
            Total number of affected rows

        Raises:
            DatabaseError: If execution fails
        """
        if not params_list:
            return 0

        self._ensure_connected()

        # Convert SQL once, then convert each params dict to list
        native_sql, _ = self._prepare_query(sql, None)
        param_names = re.findall(r":(\w+)", sql)

        with self._lock:
            try:
                if self._conn is None:
                    raise DatabaseError("Connection is not established")

                cursor = self._conn.cursor()
                total_affected = 0

                for params in params_list:
                    native_params = [params[name] for name in param_names]
                    cursor.execute(native_sql, native_params)
                    total_affected += cursor.rowcount

                # Only auto-commit if NOT inside a transaction
                if not self._in_transaction:
                    self._conn.commit()
                return total_affected
            except pyodbc.Error as e:
                if self._conn is not None and not self._in_transaction:
                    self._conn.rollback()
                log.error(f"❌ Execute many failed: {sql[:100]}...")
                raise DatabaseError("Bulk execute failed", e) from e

    # ─────────────────────────────────────────────────────────────────────────
    # MSSQL-Specific Methods
    # ─────────────────────────────────────────────────────────────────────────

    def table_exists(self, table_name: str) -> bool:
        """❓ Check if table exists (MSSQL-specific query).

        Args:
            table_name: Name of the table

        Returns:
            True if table exists
        """
        sql = """
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = :table_name
        """
        return bool(self.query(sql, {"table_name": table_name}))

    # ─────────────────────────────────────────────────────────────────────────
    # Internal Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _ensure_connected(self) -> None:
        """🔌 Ensure connection is active (lazy connect)."""
        if self._conn is None:
            self.connect()


__all__ = ["MSSQLClient"]
