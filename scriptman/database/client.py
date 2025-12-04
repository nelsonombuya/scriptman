"""🗄️ Database client base class — simple things simple."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from typing import Any

from scriptman._internal import log

# Type alias for prepared query results
PreparedQuery = tuple[str, dict[str, Any] | list[Any] | None]


class DatabaseClient(ABC):
    """🗄️ Abstract base class for database operations.

    Provides a simple, consistent interface for database access.
    Implementations handle connection details (file path, server, auth).

    Design Principles:
        - Simple methods for common operations
        - Named parameters (:name) everywhere
        - Context managers for resource management
        - Thread-safe operations

    Query Portability:
        Scriptman uses `:name` as the canonical parameter syntax. Users write
        queries once, and each backend converts to its native format internally.

        | Backend      | Native Syntax   | Example                    |
        |--------------|-----------------|----------------------------|
        | SQLite       | :name           | WHERE id = :id             |
        | PostgreSQL   | %(name)s        | WHERE id = %(id)s          |
        | MSSQL/PyODBC | ? (positional)  | WHERE id = ?               |
        | SQLAlchemy   | :name           | WHERE id = :id             |

        See `_prepare_query()` for implementation details.

    Example:
        >>> db = SQLiteClient("app.db")
        >>> db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        >>> db.execute(
        ...     "INSERT INTO users VALUES (:id, :name)",
        ...     {"id": 1, "name": "Alice"},
        ... )
        >>> users = db.query("SELECT * FROM users")
        >>> db.close()

    Context Manager:
        >>> with SQLiteClient("app.db") as db:
        ...     users = db.query("SELECT * FROM users")
    """

    def __init__(self) -> None:
        """🚀 Initialize client with contextual logging."""
        # Base class stores reference for subclass convenience
        self._log = log  # Subclasses can use self._log.debug(...)

    # ─────────────────────────────────────────────────────────────
    # Abstract Properties
    # ─────────────────────────────────────────────────────────────

    @property
    @abstractmethod
    def database_type(self) -> str:
        """🏷️ Database type identifier.

        Returns:
            Type string: 'sqlite', 'postgresql', 'mssql', etc.
        """
        ...

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """🔌 Check if connection is active.

        Returns:
            True if connected, False otherwise
        """
        ...

    # ─────────────────────────────────────────────────────────────
    # Connection Management
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def connect(self) -> None:
        """🔌 Establish database connection.

        Called automatically on first operation if not connected.
        Safe to call multiple times (idempotent).

        Raises:
            DatabaseError: If connection fails
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """🔌 Close database connection.

        Releases all resources. Safe to call multiple times.
        """
        ...

    @abstractmethod
    @contextmanager
    def transaction(self) -> Iterator[None]:
        """🔒 Transaction context manager.

        Groups multiple operations into an atomic unit.
        Commits on success, rolls back on exception.

        Example:
            >>> with db.transaction():
            ...     db.execute("INSERT INTO orders ...")
            ...     db.execute("UPDATE inventory ...")
            ...     # Commits if no exception, rolls back otherwise

        Yields:
            None
        """
        ...

    # ───────────────────────────────────────────────────────────────
    # Query Translation (Override for Non-SQLite/SQLAlchemy Backends)
    # ───────────────────────────────────────────────────────────────

    def _prepare_query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> PreparedQuery:
        """🔄 Convert canonical :name syntax to backend-native format.

        Scriptman uses `:name` as the universal parameter syntax for query
        portability. Users write queries once using `:name` placeholders,
        and each database backend converts them to its native format.

        WHY THIS EXISTS:
            Different database drivers use incompatible parameter syntaxes:

            | Backend          | Native Syntax   | Conversion From :name      |
            |------------------|-----------------|----------------------------|
            | SQLite           | :name           | Pass-through (native)      |
            | PostgreSQL       | %(name)s        | :id → %(id)s               |
            | MSSQL (PyODBC)   | ? (positional)  | :id → ? + ordered list     |
            | asyncpg          | $1, $2, ...     | :id → $1 + ordered list    |
            | SQLAlchemy       | :name           | Pass-through (native)      |

            This method enables "write once, run anywhere" database code.

        DEFAULT BEHAVIOR:
            The base implementation is a pass-through, suitable for SQLite
            and SQLAlchemy which natively support `:name` syntax.

        IMPLEMENTING FOR OTHER BACKENDS:
            Override this method to convert `:name` to your driver's format.
            The method receives the canonical SQL and params dict, and returns
            the converted SQL and params in the format your driver expects.

        Args:
            sql: SQL query with canonical :name parameters
            params: Parameter values as dictionary

        Returns:
            Tuple of (converted_sql, converted_params) where:
            - converted_sql: SQL with backend-native placeholders
            - converted_params: Parameters in backend-native format
                (dict for named, list for positional)

        Example Implementations:

            # PostgreSQL (psycopg2) - :name → %(name)s
            def _prepare_query(self, sql, params):
                if params is None:
                    return sql, params
                converted_sql = re.sub(r':(\\w+)', r'%(\\1)s', sql)
                return converted_sql, params

            # MSSQL (PyODBC) - :name → ? (positional)
            def _prepare_query(self, sql, params):
                if params is None:
                    return sql, None
                # Extract param names in order of appearance
                param_names = re.findall(r':(\\w+)', sql)
                # Replace :name with ?
                converted_sql = re.sub(r':\\w+', '?', sql)
                # Convert dict to ordered list
                converted_params = [params[name] for name in param_names]
                return converted_sql, converted_params

            # asyncpg - :name → $1, $2, ...
            def _prepare_query(self, sql, params):
                if params is None:
                    return sql, None
                param_names = re.findall(r':(\\w+)', sql)
                counter = [0]
                def replace(m):
                    counter[0] += 1
                    return f'${counter[0]}'
                converted_sql = re.sub(r':\\w+', replace, sql)
                converted_params = [params[name] for name in param_names]
                return converted_sql, converted_params

        Note:
            Implementations should call this method in query(), execute(),
            and execute_many() before passing SQL to the underlying driver.
            See SQLiteClient for reference (pass-through implementation).
        """
        # Default: pass-through (SQLite and SQLAlchemy use :name natively)
        return sql, params

    # ─────────────────────────────────────────────────────────────
    # Query Execution
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """🔍 Execute a SELECT query and return results.

        Args:
            sql: SQL query with named parameters (:name)
            params: Parameter values as dictionary

        Returns:
            List of rows as dictionaries

        Raises:
            DatabaseError: If query fails

        Example:
            >>> users = db.query(
            ...     "SELECT * FROM users WHERE age > :min_age AND status = :status",
            ...     {"min_age": 18, "status": "active"}
            ... )
            >>> for user in users:
            ...     print(user["name"], user["age"])
        """
        ...

    @abstractmethod
    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """✍️ Execute an INSERT/UPDATE/DELETE query.

        Args:
            sql: SQL query with named parameters (:name)
            params: Parameter values as dictionary

        Returns:
            Number of affected rows

        Raises:
            DatabaseError: If execution fails

        Example:
            >>> affected = db.execute(
            ...     "UPDATE users SET active = :active WHERE id = :id",
            ...     {"active": True, "id": 123}
            ... )
            >>> print(f"Updated {affected} rows")
        """
        ...

    @abstractmethod
    def execute_many(
        self,
        sql: str,
        params_list: list[dict[str, Any]],
    ) -> int:
        """✍️ Execute a query with multiple parameter sets (bulk).

        Efficiently inserts/updates many rows in a single operation.

        Args:
            sql: SQL query with named parameters
            params_list: List of parameter dictionaries

        Returns:
            Total number of affected rows

        Raises:
            DatabaseError: If execution fails

        Example:
            >>> db.execute_many(
            ...     "INSERT INTO users (name, age) VALUES (:name, :age)",
            ...     [
            ...         {"name": "Alice", "age": 30},
            ...         {"name": "Bob", "age": 25},
            ...         {"name": "Charlie", "age": 35},
            ...     ]
            ... )
        """
        ...

    # ─────────────────────────────────────────────────────────────
    # Convenience Methods (Implemented in ABC)
    # ─────────────────────────────────────────────────────────────

    def query_one(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """🔍 Execute SELECT and return first result.

        Args:
            sql: SQL query with named parameters
            params: Parameter values

        Returns:
            First row as dictionary, or None if no results

        Example:
            >>> user = db.query_one(
            ...     "SELECT * FROM users WHERE id = :id",
            ...     {"id": 123}
            ... )
            >>> if user:
            ...     print(user["name"])
        """
        results = self.query(sql, params)
        return results[0] if results else None

    def query_value(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """🔍 Execute SELECT and return single value.

        Args:
            sql: SQL query returning single column
            params: Parameter values

        Returns:
            First column of first row, or None

        Example:
            >>> count = db.query_value("SELECT COUNT(*) FROM users")
        """
        row = self.query_one(sql, params)
        if row:
            return list(row.values())[0]
        return None

    def table_exists(self, table_name: str) -> bool:
        """❓ Check if a table exists.

        Default uses INFORMATION_SCHEMA (override for SQLite).

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

    # ─────────────────────────────────────────────────────────────
    # Context Manager Protocol
    # ─────────────────────────────────────────────────────────────

    def __enter__(self) -> DatabaseClient:
        """📥 Enter context — connect."""
        self.connect()
        return self

    def __exit__(self, *args: Any) -> None:
        """📤 Exit context — close."""
        self.close()

    def __del__(self) -> None:
        """🧹 Cleanup on garbage collection."""
        with suppress(Exception):
            self.close()


__all__ = ["DatabaseClient", "PreparedQuery"]
