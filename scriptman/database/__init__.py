"""🗄️ Scriptman Database — Simple database operations.

Provides a consistent interface for database operations across
different backends (SQLite, PostgreSQL, MySQL, etc.).

Quick Start:
    >>> from scriptman.database import SQLiteClient
    >>>
    >>> # Default location: .data/db/default.db
    >>> db = SQLiteClient()
    >>>
    >>> # Custom name in default location: .data/db/myapp.db
    >>> db = SQLiteClient(name="myapp.db")
    >>>
    >>> # Explicit path (ignores config)
    >>> db = SQLiteClient("path/to/custom.db")
    >>>
    >>> db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    >>> db.execute("INSERT INTO users VALUES (:id, :name)", {"id": 1, "name": "Alice"})
    >>> users = db.query("SELECT * FROM users")
    >>> db.close()

Context Manager:
    >>> with SQLiteClient(name="app.db") as db:
    ...     users = db.query("SELECT * FROM users")

Query Portability:
    All clients use `:name` parameter syntax. Write queries once,
    run anywhere — each backend converts to its native format internally.

    >>> query = "SELECT * FROM users WHERE age > :min_age"
    >>> params = {"min_age": 18}
    >>> sqlite_db.query(query, params)    # Uses :name natively
    >>> postgres_db.query(query, params)  # Converts to %(min_age)s
    >>> mssql_db.query(query, params)     # Converts to ? positional

Available Clients:
    - SQLiteClient: Local file storage (built-in)
    - MSSQLClient: SQL Server (requires scriptman[mssql])
    - PostgresClient: PostgreSQL (requires scriptman[postgres]) — planned

Implementing Custom Clients:
    Extend DatabaseClient and override `_prepare_query()` to convert
    `:name` syntax to your driver's native format. See docstring on
    `DatabaseClient._prepare_query()` for detailed examples.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from scriptman.database.client import DatabaseClient, PreparedQuery
from scriptman.database.exceptions import DatabaseError
from scriptman.database.sqlite import SQLiteClient

if TYPE_CHECKING:
    from scriptman.database.mssql import MSSQLClient


def __getattr__(name: str) -> Any:
    """🔄 Lazy import for optional database clients.

    MSSQLClient requires pyodbc which is an optional dependency.
    This allows importing the module without pyodbc installed,
    but raises ImportError when MSSQLClient is actually used.
    """
    if name == "MSSQLClient":
        from scriptman.database.mssql import MSSQLClient

        return MSSQLClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DatabaseClient",
    "DatabaseError",
    "MSSQLClient",
    "PreparedQuery",
    "SQLiteClient",
]
