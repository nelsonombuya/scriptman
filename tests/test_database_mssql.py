"""Tests for scriptman.database.mssql — MSSQL client with mocked pyodbc.

Coverage Goals:
- Connection string building (Windows auth, SQL auth)
- Parameter conversion (:name → ?)
- Query execution flow
- Transaction commit/rollback
- Error handling and wrapping

Note: These tests use mocked pyodbc, no actual SQL Server needed.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

# ═══════════════════════════════════════════════════════════════════════════════
# MOCK SETUP - Must happen before importing MSSQLClient
# ═══════════════════════════════════════════════════════════════════════════════

# Create a persistent mock pyodbc module
_mock_pyodbc = MagicMock()
_mock_pyodbc.Error = type("PyODBCError", (Exception,), {})  # Real error class
sys.modules["pyodbc"] = _mock_pyodbc

# Now we can safely import MSSQLClient
from scriptman.database.exceptions import DatabaseError  # noqa: E402
from scriptman.database.mssql import MSSQLClient  # noqa: E402

# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture(autouse=True)
def reset_mocks():
    """🔄 Reset mocks before each test."""
    # Clear any side effects from previous tests
    _mock_pyodbc.connect.side_effect = None
    _mock_pyodbc.reset_mock()
    yield
    # Cleanup after test
    _mock_pyodbc.connect.side_effect = None


@pytest.fixture
def mock_conn():
    """🔌 Create and configure mock connection."""
    conn = MagicMock()
    cursor = MagicMock()

    # Setup cursor defaults
    cursor.description = [("id",), ("name",), ("age",)]
    cursor.fetchall.return_value = [(1, "Alice", 30), (2, "Bob", 25)]
    cursor.rowcount = 2

    # Link cursor to connection
    conn.cursor.return_value = cursor

    # Configure pyodbc.connect to return this connection
    _mock_pyodbc.connect.return_value = conn

    return {"pyodbc": _mock_pyodbc, "conn": conn, "cursor": cursor}


@pytest.fixture
def windows_client(mock_conn):
    """🏢 MSSQLClient with Windows authentication."""
    return MSSQLClient("localhost", "testdb", trusted_connection=True)


@pytest.fixture
def sql_auth_client(mock_conn):
    """🏢 MSSQLClient with SQL Server authentication."""
    return MSSQLClient("localhost", "testdb", username="sa", password="secret")


# ═══════════════════════════════════════════════════════════════════════════════
# CONNECTION STRING TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestConnectionString:
    """🔌 Test connection string building."""

    def test_windows_auth_connection_string(self, windows_client, mock_conn):
        """🔌 Should build correct connection string for Windows auth."""
        windows_client.connect()

        _mock_pyodbc.connect.assert_called_once()
        conn_str = _mock_pyodbc.connect.call_args[0][0]

        assert "Trusted_Connection=yes" in conn_str
        assert "SERVER=localhost" in conn_str
        assert "DATABASE=testdb" in conn_str
        assert "UID=" not in conn_str
        assert "PWD=" not in conn_str

    def test_sql_auth_connection_string(self, sql_auth_client, mock_conn):
        """🔌 Should build correct connection string for SQL auth."""
        sql_auth_client.connect()

        conn_str = _mock_pyodbc.connect.call_args[0][0]

        assert "UID=sa" in conn_str
        assert "PWD=secret" in conn_str
        assert "SERVER=localhost" in conn_str
        assert "DATABASE=testdb" in conn_str
        assert "Trusted_Connection" not in conn_str

    def test_custom_driver(self, mock_conn):
        """🔌 Should use custom ODBC driver."""
        client = MSSQLClient(
            "localhost",
            "testdb",
            trusted_connection=True,
            driver="{ODBC Driver 18 for SQL Server}",
        )
        client.connect()

        conn_str = _mock_pyodbc.connect.call_args[0][0]
        assert "DRIVER={ODBC Driver 18 for SQL Server}" in conn_str

    def test_named_instance(self, mock_conn):
        """🔌 Should handle named SQL Server instance."""
        client = MSSQLClient("localhost\\SQLEXPRESS", "testdb", trusted_connection=True)
        client.connect()

        conn_str = _mock_pyodbc.connect.call_args[0][0]
        assert "SERVER=localhost\\SQLEXPRESS" in conn_str


# ═══════════════════════════════════════════════════════════════════════════════
# AUTH VALIDATION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestAuthValidation:
    """🔐 Test authentication validation."""

    def test_mixed_auth_raises_error(self):
        """⚠️ Should reject both trusted_connection and username/password."""
        with pytest.raises(ValueError) as exc_info:
            MSSQLClient(
                "localhost",
                "testdb",
                username="sa",
                password="secret",
                trusted_connection=True,
            )

        assert "Cannot use both" in str(exc_info.value)

    def test_missing_auth_raises_error(self):
        """⚠️ Should reject missing authentication."""
        with pytest.raises(ValueError) as exc_info:
            MSSQLClient("localhost", "testdb")

        assert "Must provide" in str(exc_info.value)

    def test_partial_sql_auth_raises_error(self):
        """⚠️ Should reject username without password."""
        with pytest.raises(ValueError):
            MSSQLClient("localhost", "testdb", username="sa")


# ═══════════════════════════════════════════════════════════════════════════════
# PARAMETER CONVERSION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestParameterConversion:
    """🔄 Test :name to ? parameter conversion."""

    def test_single_parameter(self, windows_client):
        """🔄 Should convert single :name to ?."""
        sql, params = windows_client._prepare_query(
            "SELECT * FROM users WHERE id = :id", {"id": 123}
        )

        assert sql == "SELECT * FROM users WHERE id = ?"
        assert params == [123]

    def test_multiple_parameters(self, windows_client):
        """🔄 Should convert multiple :name to ? in order."""
        sql, params = windows_client._prepare_query(
            "SELECT * FROM users WHERE age > :min_age AND status = :status",
            {"min_age": 18, "status": "active"},
        )

        assert sql == "SELECT * FROM users WHERE age > ? AND status = ?"
        assert params == [18, "active"]

    def test_repeated_parameter(self, windows_client):
        """🔄 Should handle same parameter used multiple times."""
        sql, params = windows_client._prepare_query(
            "SELECT * FROM users WHERE created > :date OR updated > :date",
            {"date": "2024-01-01"},
        )

        assert sql == "SELECT * FROM users WHERE created > ? OR updated > ?"
        assert params == ["2024-01-01", "2024-01-01"]

    def test_no_parameters(self, windows_client):
        """🔄 Should pass through SQL without parameters."""
        sql, params = windows_client._prepare_query("SELECT * FROM users", None)

        assert sql == "SELECT * FROM users"
        assert params is None

    def test_missing_parameter_raises_error(self, windows_client):
        """⚠️ Should raise error for missing parameter."""
        with pytest.raises(DatabaseError) as exc_info:
            windows_client._prepare_query(
                "SELECT * FROM users WHERE id = :id AND name = :name",
                {"id": 123},  # Missing :name
            )

        assert "Missing parameter" in str(exc_info.value)


# ═══════════════════════════════════════════════════════════════════════════════
# QUERY EXECUTION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestQueryExecution:
    """🔍 Test query execution flow."""

    def test_query_returns_list_of_dicts(self, windows_client, mock_conn):
        """🔍 query() should return list of dictionaries."""
        results = windows_client.query("SELECT * FROM users")

        assert isinstance(results, list)
        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "Alice", "age": 30}
        assert results[1] == {"id": 2, "name": "Bob", "age": 25}

    def test_query_with_params(self, windows_client, mock_conn):
        """🔍 query() should use converted parameters."""
        windows_client.query("SELECT * FROM users WHERE age > :min_age", {"min_age": 18})

        mock_conn["cursor"].execute.assert_called_with(
            "SELECT * FROM users WHERE age > ?", [18]
        )

    def test_query_auto_connects(self, windows_client, mock_conn):
        """🔌 query() should auto-connect if not connected."""
        assert not windows_client.is_connected
        windows_client.query("SELECT 1")
        _mock_pyodbc.connect.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestExecute:
    """✍️ Test execute operations."""

    def test_execute_returns_rowcount(self, windows_client, mock_conn):
        """✍️ execute() should return affected row count."""
        mock_conn["cursor"].rowcount = 5

        affected = windows_client.execute(
            "UPDATE users SET status = :status", {"status": "inactive"}
        )

        assert affected == 5

    def test_execute_commits_outside_transaction(self, windows_client, mock_conn):
        """✍️ execute() should commit when not in transaction."""
        windows_client.execute("DELETE FROM temp")

        mock_conn["conn"].commit.assert_called_once()

    def test_execute_with_params(self, windows_client, mock_conn):
        """✍️ execute() should use converted parameters."""
        windows_client.execute(
            "INSERT INTO users (name, age) VALUES (:name, :age)",
            {"name": "Charlie", "age": 35},
        )

        mock_conn["cursor"].execute.assert_called_with(
            "INSERT INTO users (name, age) VALUES (?, ?)", ["Charlie", 35]
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TRANSACTION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestTransactions:
    """🔒 Test transaction behavior."""

    def test_transaction_commits_on_success(self, windows_client, mock_conn):
        """🔒 transaction() should commit on successful completion."""
        windows_client.connect()
        # Clear any prior commit calls from connect
        mock_conn["conn"].commit.reset_mock()

        with windows_client.transaction():
            windows_client.execute("INSERT INTO users VALUES (1, 'Test')")

        # Transaction commits once at the end
        assert mock_conn["conn"].commit.call_count == 1

    def test_transaction_rollbacks_on_exception(self, windows_client, mock_conn):
        """🔒 transaction() should rollback on user exception."""
        windows_client.connect()
        mock_conn["conn"].commit.reset_mock()
        mock_conn["conn"].rollback.reset_mock()

        with pytest.raises(ValueError):
            with windows_client.transaction():
                windows_client.execute("INSERT INTO users VALUES (1, 'Test')")
                raise ValueError("Test rollback")

        mock_conn["conn"].rollback.assert_called_once()
        mock_conn["conn"].commit.assert_not_called()

    def test_execute_skips_commit_in_transaction(self, windows_client, mock_conn):
        """🔒 execute() inside transaction should NOT auto-commit."""
        windows_client.connect()
        mock_conn["conn"].commit.reset_mock()

        with windows_client.transaction():
            windows_client.execute("INSERT INTO t1 VALUES (1)")
            windows_client.execute("INSERT INTO t2 VALUES (2)")
            # No commits should happen during this block

        # Only one commit by transaction context manager
        assert mock_conn["conn"].commit.call_count == 1

    def test_transaction_preserves_exception_type(self, windows_client, mock_conn):
        """🔒 transaction() should re-raise user exceptions unchanged."""
        windows_client.connect()

        class CustomError(Exception):
            pass

        with pytest.raises(CustomError):
            with windows_client.transaction():
                raise CustomError("Custom error")


# ═══════════════════════════════════════════════════════════════════════════════
# CONNECTION LIFECYCLE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestConnectionLifecycle:
    """🔌 Test connection management."""

    def test_is_connected_property(self, windows_client, mock_conn):
        """🔌 is_connected should reflect connection state."""
        assert not windows_client.is_connected
        windows_client.connect()
        assert windows_client.is_connected

    def test_close_releases_connection(self, windows_client, mock_conn):
        """🔌 close() should release connection."""
        windows_client.connect()
        assert windows_client.is_connected
        windows_client.close()
        assert not windows_client.is_connected
        mock_conn["conn"].close.assert_called_once()

    def test_close_idempotent(self, windows_client, mock_conn):
        """🔌 close() should be safe to call multiple times."""
        windows_client.connect()
        windows_client.close()
        windows_client.close()  # Should not raise
        windows_client.close()  # Should not raise

    def test_context_manager(self, mock_conn):
        """🔌 Context manager should connect and close."""
        with MSSQLClient("localhost", "testdb", trusted_connection=True) as db:
            db.query("SELECT 1")

        mock_conn["conn"].close.assert_called()

    def test_database_type_property(self, windows_client):
        """🏷️ database_type should return 'mssql'."""
        assert windows_client.database_type == "mssql"


# ═══════════════════════════════════════════════════════════════════════════════
# ERROR HANDLING TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestErrorHandling:
    """⚠️ Test error handling and wrapping."""

    def test_connection_error_wrapped(self, mock_conn):
        """⚠️ Connection error should be wrapped in DatabaseError."""
        # Store original return value
        original_return = _mock_pyodbc.connect.return_value

        try:
            # Must raise pyodbc.Error (not generic Exception) - that's what connect() catches
            _mock_pyodbc.connect.side_effect = _mock_pyodbc.Error("Connection refused")

            client = MSSQLClient("localhost", "testdb", trusted_connection=True)

            with pytest.raises(DatabaseError) as exc_info:
                client.connect()

            assert "Failed to connect" in str(exc_info.value)
        finally:
            # Always reset, even if test fails
            _mock_pyodbc.connect.side_effect = None
            _mock_pyodbc.connect.return_value = original_return

    def test_query_error_wrapped(self, mock_conn):
        """⚠️ Query error should be wrapped in DatabaseError."""
        # Create a fresh client for this test
        client = MSSQLClient("localhost", "testdb", trusted_connection=True)

        try:
            mock_conn["cursor"].execute.side_effect = _mock_pyodbc.Error("Invalid SQL")

            with pytest.raises(DatabaseError) as exc_info:
                client.query("INVALID SQL")

            assert "Query execution failed" in str(exc_info.value)
        finally:
            # Always reset
            mock_conn["cursor"].execute.side_effect = None


# ═══════════════════════════════════════════════════════════════════════════════
# TABLE EXISTS TEST
# ═══════════════════════════════════════════════════════════════════════════════


class TestTableExists:
    """❓ Test table_exists helper."""

    def test_table_exists_true(self, mock_conn):
        """❓ table_exists() should return True when table exists."""
        # Create fresh client
        client = MSSQLClient("localhost", "testdb", trusted_connection=True)

        # Set up description to match the SELECT 1 query result
        mock_conn["cursor"].description = [("1",)]  # Single column
        mock_conn["cursor"].fetchall.return_value = [(1,)]

        result = client.table_exists("users")

        assert result is True

    def test_table_exists_false(self, mock_conn):
        """❓ table_exists() should return False when table doesn't exist."""
        # Create fresh client
        client = MSSQLClient("localhost", "testdb", trusted_connection=True)

        # Set up description to match the SELECT 1 query (even though no rows)
        mock_conn["cursor"].description = [("1",)]
        mock_conn["cursor"].fetchall.return_value = []

        result = client.table_exists("nonexistent")

        assert result is False
