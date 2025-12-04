# 🗄️ Database Module — Implementation Plan

**Status:** ✅ Implemented
**Module:** `scriptman.database`
**Last Updated:** 2024-12-04

---

## 📋 Implementation Progress

| Component                | Status | Description                             |
| ------------------------ | ------ | --------------------------------------- |
| `DatabaseClient` ABC     | ✅      | Base class with `_prepare_query()` hook |
| `DatabaseError`          | ✅      | Exception wrapper with original error   |
| `SQLiteClient`           | ✅      | Full SQLite implementation              |
| `tests/test_database.py` | ✅      | Comprehensive test suite (70+ tests)    |
| `MSSQLClient`            | ✅      | PyODBC implementation                   |
| `pyproject.toml` update  | ✅      | Added `mssql` optional dependency       |

---

## Overview

The database module provides a simple, consistent interface for database operations across different backends. SQLite is the default (and only built-in) backend, with MSSQL via PyODBC as the next extension.

### Design Principles

1. **Simple Things Simple** — `SQLiteClient()` works with zero config
2. **Query Portability** — Write queries once with `:name` syntax, run anywhere
3. **Thread Safety** — All operations protected by internal locking
4. **Resource Management** — Context managers, `__del__`, and explicit `close()`
5. **Observability** — Uses `_internal.log` for consistent telemetry

---

## File Structure

### Current Structure
```
scriptman/database/
├── __init__.py      # 🗄️ Module exports and quick start docs
├── client.py        # 🗄️ ABC defining DatabaseClient interface
├── exceptions.py    # 🚨 DatabaseError exception
└── sqlite.py        # 💾 SQLiteClient implementation
```

### Target Structure (After MSSQLClient)
```
scriptman/database/
├── __init__.py      # 🗄️ Module exports (add MSSQLClient)
├── client.py        # 🗄️ ABC defining DatabaseClient interface
├── exceptions.py    # 🚨 DatabaseError exception
├── sqlite.py        # 💾 SQLiteClient implementation
└── mssql.py         # 🏢 MSSQLClient implementation (NEW)
```

---

## Public API

### Current Exports (`__all__`)

| Export           | Type      | Description                                       |
| ---------------- | --------- | ------------------------------------------------- |
| `DatabaseClient` | ABC       | Base class for database backends                  |
| `DatabaseError`  | Exception | Wrapped database errors with context              |
| `PreparedQuery`  | TypeAlias | `tuple[str, dict[str, Any] \| list[Any] \| None]` |
| `SQLiteClient`   | Class     | SQLite implementation                             |

### Planned Exports (After MSSQLClient)

| Export        | Type  | Description           |
| ------------- | ----- | --------------------- |
| `MSSQLClient` | Class | SQL Server via PyODBC |

---

## SQLiteClient (✅ Implemented)

**Zero-Config Usage:**
```python
from scriptman.database import SQLiteClient

# Default: .data/db/default.db
db = SQLiteClient()

# Named database: .data/db/myapp.db
db = SQLiteClient(name="myapp.db")

# Explicit path
db = SQLiteClient("path/to/custom.db")

# In-memory
db = SQLiteClient(":memory:")
```

**Context Manager:**
```python
with SQLiteClient("app.db") as db:
    users = db.query("SELECT * FROM users")
```

**Named Parameters (Portable):**
```python
# Works with SQLite, PostgreSQL, MSSQL — each converts internally
db.query(
    "SELECT * FROM users WHERE age > :min_age AND status = :status",
    {"min_age": 18, "status": "active"}
)
```

---

## Query Portability Pattern

Scriptman uses `:name` as the canonical parameter syntax. Each backend converts to its native format:

| Backend      | Native Syntax    | Conversion                  |
| ------------ | ---------------- | --------------------------- |
| SQLite       | `:name`          | Pass-through                |
| PostgreSQL   | `%(name)s`       | `:id` → `%(id)s`            |
| MSSQL/PyODBC | `?` (positional) | `:id` → `?` + ordered list  |
| asyncpg      | `$1, $2, ...`    | `:id` → `$1` + ordered list |
| SQLAlchemy   | `:name`          | Pass-through                |

**Implementation:** Override `_prepare_query()` in custom backends. See `client.py` docstring for examples.

---

## Thread Safety

- All database client operations protected by `threading.RLock()` (reentrant for transactions)
- WAL mode enabled by default for better concurrent read performance
- `check_same_thread=False` in SQLite connect (we handle safety with lock)

---

## WAL Mode

WAL (Write-Ahead Logging) is **enabled automatically on connect**.

| Method               | Description                            |
| -------------------- | -------------------------------------- |
| `enable_wal_mode()`  | Enable WAL (called automatically)      |
| `disable_wal_mode()` | Switch to traditional rollback journal |
| `is_wal_mode()`      | Check current mode                     |

**When to disable:** Single-writer, low-concurrency workloads where you want simpler recovery.

---

## Helper Methods

| Method             | Emoji | Description                    |
| ------------------ | ----- | ------------------------------ |
| `query()`          | 🔍     | SELECT with dict results       |
| `query_one()`      | 🔍     | SELECT first row or None       |
| `query_value()`    | 🔍     | SELECT single value            |
| `execute()`        | ✍️     | INSERT/UPDATE/DELETE           |
| `execute_many()`   | ✍️     | Bulk operations                |
| `execute_script()` | 📜     | Multi-statement DDL            |
| `transaction()`    | 🔒     | Context manager for atomic ops |
| `create_table()`   | 🏗️     | Programmatic table creation    |
| `drop_table()`     | 🗑️     | Drop table                     |
| `create_index()`   | 📇     | Create index                   |
| `table_exists()`   | ❓     | Check table existence          |
| `list_tables()`    | 📋     | List all tables                |
| `vacuum()`         | 🧹     | Reclaim space                  |
| `get_pragma()`     | ⚙️     | Get SQLite pragma              |
| `set_pragma()`     | ⚙️     | Set SQLite pragma              |

---

## Error Handling

All database errors wrapped in `DatabaseError`:

```python
try:
    db.execute("INVALID SQL")
except DatabaseError as e:
    print(f"Database error: {e}")
    print(f"Original: {e.original}")  # sqlite3.Error or pyodbc.Error
```

---

## Compliance Checklist (SQLiteClient)

| Requirement               | Status                                    |
| ------------------------- | ----------------------------------------- |
| Zero-config usage         | ✅ `SQLiteClient()` uses default path      |
| Advanced options optional | ✅ `name`, `timeout` kwargs                |
| Type hints (3.12+ syntax) | ✅ `str \| Path \| None`, `dict[str, Any]` |
| Emoji docstrings          | ✅ All 38+ public methods                  |
| Error handling            | ✅ `DatabaseError` wraps originals         |
| Resource cleanup          | ✅ `close()`, `__del__`, context manager   |
| Variable visibility       | ✅ `_conn`, `_lock`, `_path` protected     |
| No magic attributes       | ✅ Explicit methods only                   |
| Uses `_internal.log`      | ✅ Both `client.py` and `sqlite.py`        |
| ABCs over Protocols       | ✅ `DatabaseClient(ABC)`                   |
| Thread safety             | ✅ `RLock()` on all operations             |
| Transaction isolation     | ✅ `_in_transaction` flag for atomic ops   |
| Raises documented         | ✅ All helper methods                      |
| Tests (95%+)              | ✅ 70+ tests implemented                   |

---

## Fixes Applied (2024-12-04)

1. **`client.py`**: Changed from `loguru.logger` to `_internal.log`
2. **`sqlite.py`**: Fixed `disable_wal_mode()` docstring (was copy-paste error)
3. **`sqlite.py`**: Changed `enable_wal_mode()`/`disable_wal_mode()` log level to DEBUG
4. **`sqlite.py`**: Fixed class docstring WAL note redundancy
5. **`sqlite.py`**: Added `Raises:` to helper method docstrings

---

# 📋 Step 1: Tests Implementation

**File:** `tests/test_database.py`
**Target Coverage:** 95%+

## Test File Structure

```python
"""Tests for scriptman.database — SQLite database client.

Coverage Goals:
- Connection lifecycle (connect/close/reconnect/context manager)
- CRUD operations (query, execute, execute_many)
- Transactions (commit on success, rollback on error)
- Helper methods (create_table, drop_table, create_index, etc.)
- WAL mode operations
- Thread safety
- Edge cases (unicode, empty results, large data)
- Error handling (DatabaseError wrapping)

Target: 95%+ coverage
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from scriptman.database import DatabaseClient, DatabaseError, SQLiteClient
```

## Fixtures

```python
# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def memory_db():
    """📦 In-memory SQLite database for fast tests."""
    db = SQLiteClient(":memory:")
    yield db
    db.close()


@pytest.fixture
def memory_db_with_table(memory_db):
    """📦 In-memory database with a test table."""
    memory_db.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT
        )
    """)
    yield memory_db


@pytest.fixture
def memory_db_with_data(memory_db_with_table):
    """📦 In-memory database with test data."""
    memory_db_with_table.execute_many(
        "INSERT INTO users (id, name, age, email) VALUES (:id, :name, :age, :email)",
        [
            {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"},
            {"id": 3, "name": "Charlie", "age": 35, "email": None},
        ]
    )
    yield memory_db_with_table


@pytest.fixture
def temp_db(tmp_path):
    """📁 File-based SQLite database in temp directory."""
    db_path = tmp_path / "test.db"
    db = SQLiteClient(str(db_path))
    yield db
    db.close()
```

## Test Classes

### Connection Lifecycle

```python
# ═══════════════════════════════════════════════════════════════════════════════
# CONNECTION LIFECYCLE
# ═══════════════════════════════════════════════════════════════════════════════

class TestConnection:
    """🔌 Test connection management."""

    def test_connect_creates_connection(self):
        """🔌 connect() should establish database connection."""

    def test_close_releases_connection(self):
        """🔌 close() should release database connection."""

    def test_close_idempotent(self):
        """🔌 close() should be safe to call multiple times."""

    def test_context_manager_connects_and_closes(self):
        """🔌 Context manager should connect on enter, close on exit."""

    def test_reconnect_after_close(self):
        """🔌 Should be able to reconnect after closing."""

    def test_is_connected_property(self):
        """🔌 is_connected should reflect connection state."""

    def test_database_type_property(self):
        """🏷️ database_type should return 'sqlite'."""

    def test_path_property(self):
        """📁 path property should return database path."""

    def test_lazy_connect_on_query(self):
        """🔌 Should auto-connect on first query."""

    def test_del_closes_connection(self):
        """🧹 __del__ should close connection gracefully."""


class TestPathResolution:
    """📁 Test database path resolution."""

    def test_memory_database(self):
        """📁 ':memory:' should create in-memory database."""

    def test_explicit_path(self):
        """📁 Explicit path should be used as-is."""

    def test_explicit_path_creates_parent_dirs(self):
        """📁 Explicit path should create parent directories."""

    def test_default_path_with_name(self):
        """📁 Default path with name should use config directory."""

    def test_default_path_creates_directory(self):
        """📁 Default path should create directory if needed."""
```

### Query Operations

```python
# ═══════════════════════════════════════════════════════════════════════════════
# QUERY OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════

class TestQueryOperations:
    """🔍 Test SELECT queries."""

    def test_query_returns_list_of_dicts(self, memory_db_with_data):
        """🔍 query() should return list of dictionaries."""

    def test_query_with_params(self, memory_db_with_data):
        """🔍 query() should accept named parameters."""

    def test_query_empty_result(self, memory_db_with_table):
        """🔍 query() should return empty list when no results."""

    def test_query_one_returns_first_row(self, memory_db_with_data):
        """🔍 query_one() should return first row as dict."""

    def test_query_one_returns_none_when_empty(self, memory_db_with_table):
        """🔍 query_one() should return None when no results."""

    def test_query_value_returns_single_value(self, memory_db_with_data):
        """🔍 query_value() should return single scalar value."""

    def test_query_value_returns_none_when_empty(self, memory_db_with_table):
        """🔍 query_value() should return None when no results."""

    def test_query_preserves_column_order(self, memory_db_with_data):
        """🔍 query() should preserve column order in results."""
```

### Execute Operations

```python
# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecuteOperations:
    """✍️ Test INSERT/UPDATE/DELETE operations."""

    def test_execute_insert(self, memory_db_with_table):
        """✍️ execute() should insert rows."""

    def test_execute_update(self, memory_db_with_data):
        """✍️ execute() should update rows."""

    def test_execute_delete(self, memory_db_with_data):
        """✍️ execute() should delete rows."""

    def test_execute_returns_affected_rows(self, memory_db_with_data):
        """✍️ execute() should return count of affected rows."""

    def test_execute_with_params(self, memory_db_with_table):
        """✍️ execute() should accept named parameters."""

    def test_execute_many_bulk_insert(self, memory_db_with_table):
        """✍️ execute_many() should bulk insert rows."""

    def test_execute_many_returns_total_affected(self, memory_db_with_table):
        """✍️ execute_many() should return total affected rows."""

    def test_execute_many_empty_list(self, memory_db_with_table):
        """✍️ execute_many() with empty list should return 0."""

    def test_execute_script_multiple_statements(self, memory_db):
        """📜 execute_script() should run multiple statements."""
```

### Transactions

```python
# ═══════════════════════════════════════════════════════════════════════════════
# TRANSACTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class TestTransactions:
    """🔒 Test transaction behavior."""

    def test_transaction_commits_on_success(self, memory_db_with_table):
        """🔒 transaction() should commit on successful completion."""

    def test_transaction_rollbacks_on_exception(self, memory_db_with_table):
        """🔒 transaction() should rollback on user exception."""

    def test_transaction_rollbacks_on_database_error(self, memory_db_with_table):
        """🔒 transaction() should rollback on database error."""

    def test_transaction_preserves_user_exception_type(self, memory_db_with_table):
        """🔒 transaction() should re-raise user exceptions unchanged."""

    def test_nested_operations_in_transaction(self, memory_db_with_table):
        """🔒 Multiple operations in transaction should be atomic."""
```

### Table Operations

```python
# ═══════════════════════════════════════════════════════════════════════════════
# TABLE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════

class TestTableOperations:
    """🏗️ Test table management."""

    def test_create_table(self, memory_db):
        """🏗️ create_table() should create a new table."""

    def test_create_table_with_primary_key(self, memory_db):
        """🏗️ create_table() should support primary keys."""

    def test_create_table_with_composite_primary_key(self, memory_db):
        """🏗️ create_table() should support composite primary keys."""

    def test_create_table_if_not_exists(self, memory_db):
        """🏗️ create_table() with if_not_exists should not fail."""

    def test_drop_table(self, memory_db_with_table):
        """🗑️ drop_table() should remove table."""

    def test_drop_table_if_exists(self, memory_db):
        """🗑️ drop_table() with if_exists should not fail."""

    def test_table_exists_true(self, memory_db_with_table):
        """❓ table_exists() should return True for existing table."""

    def test_table_exists_false(self, memory_db):
        """❓ table_exists() should return False for non-existent table."""

    def test_list_tables(self, memory_db_with_table):
        """📋 list_tables() should return list of table names."""

    def test_list_tables_empty(self, memory_db):
        """📋 list_tables() should return empty list when no tables."""

    def test_create_index(self, memory_db_with_table):
        """📇 create_index() should create an index."""

    def test_create_unique_index(self, memory_db_with_table):
        """📇 create_index() should support unique indexes."""
```

### WAL Mode

```python
# ═══════════════════════════════════════════════════════════════════════════════
# WAL MODE
# ═══════════════════════════════════════════════════════════════════════════════

class TestWALMode:
    """⚡ Test WAL mode operations."""

    def test_wal_mode_enabled_by_default(self, temp_db):
        """⚡ WAL mode should be enabled by default on connect."""

    def test_disable_wal_mode(self, temp_db):
        """🔒 disable_wal_mode() should switch to DELETE journal."""

    def test_enable_wal_mode(self, temp_db):
        """⚡ enable_wal_mode() should switch to WAL journal."""

    def test_is_wal_mode_true(self, temp_db):
        """🔍 is_wal_mode() should return True when WAL enabled."""

    def test_is_wal_mode_false(self, temp_db):
        """🔍 is_wal_mode() should return False when WAL disabled."""
```

### Pragmas

```python
# ═══════════════════════════════════════════════════════════════════════════════
# PRAGMAS
# ═══════════════════════════════════════════════════════════════════════════════

class TestPragmas:
    """⚙️ Test pragma operations."""

    def test_get_pragma(self, memory_db):
        """⚙️ get_pragma() should return pragma value."""

    def test_set_pragma(self, memory_db):
        """⚙️ set_pragma() should set pragma value."""
```

### Edge Cases

```python
# ═══════════════════════════════════════════════════════════════════════════════
# EDGE CASES
# ═══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    """🔬 Test edge cases and special scenarios."""

    def test_unicode_data(self, memory_db_with_table):
        """🔤 Should handle unicode characters correctly."""

    def test_emoji_data(self, memory_db_with_table):
        """😀 Should handle emoji characters correctly."""

    def test_empty_string_value(self, memory_db_with_table):
        """📝 Should handle empty string values."""

    def test_null_value(self, memory_db_with_table):
        """∅ Should handle NULL values correctly."""

    def test_large_text_value(self, memory_db_with_table):
        """📚 Should handle large text values."""

    def test_special_characters(self, memory_db_with_table):
        """🔣 Should handle special characters (quotes, newlines)."""

    def test_numeric_precision(self, memory_db):
        """🔢 Should preserve numeric precision."""

    def test_blob_data(self, memory_db):
        """📦 Should handle BLOB data correctly."""
```

### Error Handling

```python
# ═══════════════════════════════════════════════════════════════════════════════
# ERROR HANDLING
# ═══════════════════════════════════════════════════════════════════════════════

class TestErrorHandling:
    """⚠️ Test error handling and DatabaseError wrapping."""

    def test_invalid_sql_raises_database_error(self, memory_db):
        """⚠️ Invalid SQL should raise DatabaseError."""

    def test_database_error_wraps_original(self, memory_db):
        """⚠️ DatabaseError should wrap original exception."""

    def test_database_error_includes_message(self, memory_db):
        """⚠️ DatabaseError should include helpful message."""

    def test_missing_param_raises_error(self, memory_db_with_table):
        """⚠️ Missing parameter should raise error."""

    def test_constraint_violation_raises_error(self, memory_db_with_table):
        """⚠️ Constraint violation should raise DatabaseError."""
```

### Thread Safety

```python
# ═══════════════════════════════════════════════════════════════════════════════
# THREAD SAFETY
# ═══════════════════════════════════════════════════════════════════════════════

class TestThreadSafety:
    """🔐 Test thread safety with concurrent access."""

    def test_concurrent_reads(self, temp_db):
        """🔐 Concurrent reads should be thread-safe."""

    def test_concurrent_writes(self, temp_db):
        """🔐 Concurrent writes should be thread-safe."""

    def test_concurrent_read_write(self, temp_db):
        """🔐 Concurrent read/write should be thread-safe."""
```

### Vacuum

```python
# ═══════════════════════════════════════════════════════════════════════════════
# VACUUM
# ═══════════════════════════════════════════════════════════════════════════════

class TestVacuum:
    """🧹 Test database optimization."""

    def test_vacuum(self, temp_db):
        """🧹 vacuum() should optimize database file."""
```

---

# 📋 Step 2: MSSQLClient Implementation

**File:** `scriptman/database/mssql.py`
**Dependency:** `pyodbc>=5.0` (optional extra)

## Connection Pooling Background

### What is Connection Pooling?

Connection pooling reuses database connections instead of creating new ones for each operation.

**Without Pooling:**
```
Request 1: Open connection → Execute → Close connection  (50-200ms)
Request 2: Open connection → Execute → Close connection  (50-200ms)
Request 3: Open connection → Execute → Close connection  (50-200ms)
```

**With Pooling:**
```
Startup: Create pool of N connections

Request 1: Borrow from pool → Execute → Return to pool  (1-5ms)
Request 2: Borrow from pool → Execute → Return to pool  (1-5ms)
Request 3: Borrow from pool → Execute → Return to pool  (1-5ms)
```

### Why It Matters for MSSQL

| Aspect                | Without Pooling       | With Pooling       |
| --------------------- | --------------------- | ------------------ |
| **Latency**           | ~50-200ms per connect | ~1-5ms (borrow)    |
| **Server Load**       | Many auth requests    | Few auth requests  |
| **Connection Limits** | Risk hitting max      | Controlled size    |
| **Reliability**       | Frequent failures     | Stable connections |

### Implementation Approach

**Start Simple:** PyODBC has built-in pooling (enabled by default on Windows).

```python
# PyODBC pooling is automatic
pyodbc.pooling = True  # Default on Windows
```

**Future Enhancement:** Add explicit pool control if needed.

## MSSQLClient Implementation

```python
"""🏢 Microsoft SQL Server client via PyODBC."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from threading import Lock
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
        username: SQL Server username (for SQL auth, mutually exclusive with trusted_connection)
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
        Use `:name` syntax for parameters — automatically converted to `?` placeholders:
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
                "⚠️ Must provide either trusted_connection=True or both username and password."
            )

        self._server = server
        self._database = database
        self._username = username
        self._password = password
        self._trusted_connection = trusted_connection
        self._driver = driver
        self._timeout = timeout
        self._conn: pyodbc.Connection | None = None
        self._lock = Lock()

    # ─────────────────────────────────────────────────────────────
    # Properties
    # ─────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────
    # Connection Management
    # ─────────────────────────────────────────────────────────────

    def _build_connection_string(self) -> str:
        """🔧 Build ODBC connection string."""
        parts = [
            f"DRIVER={self._driver}",
            f"SERVER={self._server}",
            f"DATABASE={self._database}",
            f"TIMEOUT={self._timeout}",
        ]

        if self._trusted_connection:
            parts.append("Trusted_Connection=yes")
        else:
            parts.append(f"UID={self._username}")
            parts.append(f"PWD={self._password}")

        return ";".join(parts)

    def connect(self) -> None:
        """🔌 Establish MSSQL connection."""
        if self._conn is not None:
            return  # Already connected

        try:
            conn_str = self._build_connection_string()
            self._conn = pyodbc.connect(conn_str, timeout=self._timeout)
            log.debug(f"🔌 Connected to MSSQL: {self._server}/{self._database}")
        except pyodbc.Error as e:
            log.critical(f"🔥 Failed to connect to MSSQL: {self._server}/{self._database}")
            raise DatabaseError(f"Failed to connect to {self._server}/{self._database}", e)

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
        """🔒 Transaction context manager."""
        self._ensure_connected()

        if self._conn is None:
            raise DatabaseError("Connection is not established")

        with self._lock:
            try:
                yield
                self._conn.commit()
            except pyodbc.Error as e:
                self._conn.rollback()
                raise DatabaseError("Transaction failed", e)
            except Exception:
                self._conn.rollback()
                raise

    # ─────────────────────────────────────────────────────────────
    # Query Translation (:name → ?)
    # ─────────────────────────────────────────────────────────────

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
        """
        if params is None:
            return sql, None

        # Extract param names in order of appearance
        param_names = re.findall(r':(\w+)', sql)

        # Replace :name with ?
        converted_sql = re.sub(r':\w+', '?', sql)

        # Convert dict to ordered list matching placeholder order
        try:
            converted_params = [params[name] for name in param_names]
        except KeyError as e:
            raise DatabaseError(f"Missing parameter: {e}")

        return converted_sql, converted_params

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
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            except pyodbc.Error as e:
                log.error(f"❌ Query failed: {sql[:100]}...")
                raise DatabaseError("Query execution failed", e)

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """✍️ Execute INSERT/UPDATE/DELETE and return affected rows."""
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
                self._conn.commit()
                return cursor.rowcount
            except pyodbc.Error as e:
                if self._conn is not None:
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

        # Convert SQL once, then convert each params dict to list
        native_sql, _ = self._prepare_query(sql, None)
        param_names = re.findall(r':(\w+)', sql)

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

                self._conn.commit()
                return total_affected
            except pyodbc.Error as e:
                if self._conn is not None:
                    self._conn.rollback()
                log.error(f"❌ Execute many failed: {sql[:100]}...")
                raise DatabaseError("Bulk execute failed", e)

    # ─────────────────────────────────────────────────────────────
    # MSSQL-Specific Methods
    # ─────────────────────────────────────────────────────────────

    def table_exists(self, table_name: str) -> bool:
        """❓ Check if table exists (MSSQL-specific query)."""
        sql = """
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = :table_name
        """
        return bool(self.query(sql, {"table_name": table_name}))

    # ─────────────────────────────────────────────────────────────
    # Internal Helpers
    # ─────────────────────────────────────────────────────────────

    def _ensure_connected(self) -> None:
        """🔌 Ensure connection is active (lazy connect)."""
        if self._conn is None:
            self.connect()


__all__ = ["MSSQLClient"]
```

## pyproject.toml Changes

```toml
[project.optional-dependencies]
# Database backends
mssql = ["pyodbc>=5.0"]

# All optional features (add mssql)
all = [
    "tomlkit>=0.13.2",
    "pyyaml>=6.0",
    "python-dotenv>=1.0",
    "fastapi>=0.100.0",
    "uvicorn>=0.23.0",
    "pyodbc>=5.0",
]
```

## __init__.py Changes

```python
# Add lazy import for MSSQLClient
def __getattr__(name: str) -> Any:
    if name == "MSSQLClient":
        from scriptman.database.mssql import MSSQLClient
        return MSSQLClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

# Update __all__
__all__ = [
    "DatabaseClient",
    "DatabaseError",
    "PreparedQuery",
    "SQLiteClient",
    "MSSQLClient",  # Lazy-loaded
]
```

---

# 📋 Step 3: MSSQLClient Tests

**File:** `tests/test_database_mssql.py` (separate file for optional dependency)

## Test Strategy

Since PyODBC requires SQL Server, tests should:
1. **Mock PyODBC** for unit tests (no real database needed)
2. **Integration tests** marked with `@pytest.mark.integration` for real database

```python
"""Tests for scriptman.database.mssql — MSSQL client via PyODBC.

Note: These tests mock pyodbc. Integration tests require a real SQL Server.

Coverage Goals:
- Connection string building (Windows auth, SQL auth)
- Query parameter conversion (:name → ?)
- CRUD operations via mocked pyodbc
- Error handling
"""

import pytest
from unittest.mock import MagicMock, patch

# Skip all tests if pyodbc not installed
pytest.importorskip("pyodbc")

from scriptman.database import MSSQLClient, DatabaseError


class TestConnectionString:
    """🔧 Test connection string building."""

    def test_windows_auth_connection_string(self):
        """🔧 Windows auth should include Trusted_Connection."""

    def test_sql_auth_connection_string(self):
        """🔧 SQL auth should include UID and PWD."""

    def test_named_instance_in_connection_string(self):
        """🔧 Named instance should be preserved in server."""


class TestAuthValidation:
    """🔐 Test authentication configuration validation."""

    def test_requires_auth_method(self):
        """🔐 Should raise if no auth method provided."""

    def test_cannot_use_both_auth_methods(self):
        """🔐 Should raise if both Windows and SQL auth provided."""


class TestQueryConversion:
    """🔄 Test :name → ? parameter conversion."""

    def test_single_param_conversion(self):
        """🔄 Single :name should convert to ?."""

    def test_multiple_params_conversion(self):
        """🔄 Multiple :names should convert to ?s in order."""

    def test_repeated_param_conversion(self):
        """🔄 Repeated :name should each become ?."""

    def test_no_params_passthrough(self):
        """🔄 No params should pass SQL unchanged."""

    def test_missing_param_raises_error(self):
        """🔄 Missing param in dict should raise error."""
```

---

## Running Tests

```bash
# Run all database tests
python -m poetry run pytest tests/test_database.py -v --no-cov

# Run with coverage
python -m poetry run pytest tests/test_database.py --cov=scriptman.database --cov-report=term-missing

# Run MSSQL tests (requires pyodbc)
python -m poetry run pytest tests/test_database_mssql.py -v --no-cov

# Run integration tests (requires real SQL Server)
python -m poetry run pytest tests/test_database_mssql.py -v -m integration
```

---

## Future Extensions

### PostgresClient (Planned)
```python
# scriptman[postgres]
from scriptman.database import PostgresClient

db = PostgresClient(
    host="localhost",
    database="mydb",
    user="user",
    password="pass",
)
```

### Async Support (Future)
```python
# Potential async interface
async with AsyncSQLiteClient(":memory:") as db:
    users = await db.query("SELECT * FROM users")
```

### Explicit Connection Pooling (Future)
```python
# For high-throughput scenarios
db = MSSQLClient(
    server="localhost",
    database="mydb",
    pool_size=10,
    pool_recycle=3600,
)
```
