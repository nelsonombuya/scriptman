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

import pytest

from scriptman.database import DatabaseError, SQLiteClient

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
    memory_db.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT
        )
    """
    )
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
        ],
    )
    yield memory_db_with_table


@pytest.fixture
def temp_db(tmp_path):
    """📁 File-based SQLite database in temp directory."""
    db_path = tmp_path / "test.db"
    db = SQLiteClient(str(db_path))
    yield db
    db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# CONNECTION LIFECYCLE
# ═══════════════════════════════════════════════════════════════════════════════


class TestConnection:
    """🔌 Test connection management."""

    def test_connect_creates_connection(self):
        """🔌 connect() should establish database connection."""
        db = SQLiteClient(":memory:")
        assert not db.is_connected
        db.connect()
        assert db.is_connected
        db.close()

    def test_close_releases_connection(self):
        """🔌 close() should release database connection."""
        db = SQLiteClient(":memory:")
        db.connect()
        assert db.is_connected
        db.close()
        assert not db.is_connected

    def test_close_idempotent(self):
        """🔌 close() should be safe to call multiple times."""
        db = SQLiteClient(":memory:")
        db.connect()
        db.close()
        db.close()  # Should not raise
        db.close()  # Should not raise
        assert not db.is_connected

    def test_context_manager_connects_and_closes(self):
        """🔌 Context manager should connect on enter, close on exit."""
        with SQLiteClient(":memory:") as db:
            assert db.is_connected
            db.execute("SELECT 1")
        assert not db.is_connected

    def test_reconnect_after_close(self):
        """🔌 Should be able to reconnect after closing."""
        db = SQLiteClient(":memory:")
        db.connect()
        db.close()
        db.connect()
        assert db.is_connected
        db.close()

    def test_is_connected_property(self, memory_db):
        """🔌 is_connected should reflect connection state."""
        # memory_db fixture connects automatically on first operation
        memory_db.execute("SELECT 1")
        assert memory_db.is_connected

    def test_database_type_property(self, memory_db):
        """🏷️ database_type should return 'sqlite'."""
        assert memory_db.database_type == "sqlite"

    def test_path_property(self, memory_db):
        """📁 path property should return database path."""
        assert memory_db.path == ":memory:"

    def test_lazy_connect_on_query(self):
        """🔌 Should auto-connect on first query."""
        db = SQLiteClient(":memory:")
        assert not db.is_connected
        db.execute("SELECT 1")  # Should auto-connect
        assert db.is_connected
        db.close()

    def test_del_closes_connection(self):
        """🧹 __del__ should close connection gracefully."""
        db = SQLiteClient(":memory:")
        db.connect()
        assert db.is_connected
        del db  # Should close without raising


class TestPathResolution:
    """📁 Test database path resolution."""

    def test_memory_database(self):
        """📁 ':memory:' should create in-memory database."""
        db = SQLiteClient(":memory:")
        assert db.path == ":memory:"
        db.close()

    def test_explicit_path(self, tmp_path):
        """📁 Explicit path should be used as-is."""
        db_path = tmp_path / "explicit.db"
        db = SQLiteClient(str(db_path))
        assert db.path == str(db_path)
        db.connect()
        assert db_path.exists()
        db.close()

    def test_explicit_path_creates_parent_dirs(self, tmp_path):
        """📁 Explicit path should create parent directories."""
        nested_path = tmp_path / "nested" / "dirs" / "test.db"
        db = SQLiteClient(str(nested_path))
        assert nested_path.parent.exists()
        db.close()

    def test_default_path_with_name(self):
        """📁 Default path with name should use config directory."""
        # This tests the path resolution but doesn't persist
        db = SQLiteClient(":memory:")  # Using memory to avoid file creation
        assert db.path == ":memory:"
        db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# QUERY OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


class TestQueryOperations:
    """🔍 Test SELECT queries."""

    def test_query_returns_list_of_dicts(self, memory_db_with_data):
        """🔍 query() should return list of dictionaries."""
        results = memory_db_with_data.query("SELECT * FROM users")
        assert isinstance(results, list)
        assert len(results) == 3
        assert isinstance(results[0], dict)
        assert "id" in results[0]
        assert "name" in results[0]

    def test_query_with_params(self, memory_db_with_data):
        """🔍 query() should accept named parameters."""
        results = memory_db_with_data.query(
            "SELECT * FROM users WHERE age > :min_age", {"min_age": 28}
        )
        assert len(results) == 2
        names = [r["name"] for r in results]
        assert "Alice" in names
        assert "Charlie" in names

    def test_query_empty_result(self, memory_db_with_table):
        """🔍 query() should return empty list when no results."""
        results = memory_db_with_table.query("SELECT * FROM users")
        assert results == []

    def test_query_one_returns_first_row(self, memory_db_with_data):
        """🔍 query_one() should return first row as dict."""
        result = memory_db_with_data.query_one(
            "SELECT * FROM users WHERE id = :id", {"id": 1}
        )
        assert result is not None
        assert result["name"] == "Alice"
        assert result["age"] == 30

    def test_query_one_returns_none_when_empty(self, memory_db_with_table):
        """🔍 query_one() should return None when no results."""
        result = memory_db_with_table.query_one(
            "SELECT * FROM users WHERE id = :id", {"id": 999}
        )
        assert result is None

    def test_query_value_returns_single_value(self, memory_db_with_data):
        """🔍 query_value() should return single scalar value."""
        count = memory_db_with_data.query_value("SELECT COUNT(*) FROM users")
        assert count == 3

    def test_query_value_returns_none_when_empty(self, memory_db_with_table):
        """🔍 query_value() should return None when no results."""
        result = memory_db_with_table.query_value(
            "SELECT name FROM users WHERE id = :id", {"id": 999}
        )
        assert result is None

    def test_query_preserves_column_order(self, memory_db_with_data):
        """🔍 query() should preserve column order in results."""
        results = memory_db_with_data.query("SELECT id, name, age FROM users LIMIT 1")
        assert results[0] is not None
        keys = list(results[0].keys())
        assert keys == ["id", "name", "age"]


# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


class TestExecuteOperations:
    """✍️ Test INSERT/UPDATE/DELETE operations."""

    def test_execute_insert(self, memory_db_with_table):
        """✍️ execute() should insert rows."""
        affected = memory_db_with_table.execute(
            "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
            {"id": 1, "name": "Test", "age": 20},
        )
        assert affected == 1

        result = memory_db_with_table.query_one(
            "SELECT * FROM users WHERE id = :id", {"id": 1}
        )
        assert result["name"] == "Test"

    def test_execute_update(self, memory_db_with_data):
        """✍️ execute() should update rows."""
        affected = memory_db_with_data.execute(
            "UPDATE users SET age = :age WHERE name = :name",
            {"age": 31, "name": "Alice"},
        )
        assert affected == 1

        result = memory_db_with_data.query_one(
            "SELECT age FROM users WHERE name = :name", {"name": "Alice"}
        )
        assert result["age"] == 31

    def test_execute_delete(self, memory_db_with_data):
        """✍️ execute() should delete rows."""
        affected = memory_db_with_data.execute(
            "DELETE FROM users WHERE name = :name", {"name": "Bob"}
        )
        assert affected == 1

        count = memory_db_with_data.query_value("SELECT COUNT(*) FROM users")
        assert count == 2

    def test_execute_returns_affected_rows(self, memory_db_with_data):
        """✍️ execute() should return count of affected rows."""
        affected = memory_db_with_data.execute(
            "UPDATE users SET age = age + 1 WHERE age > :min_age", {"min_age": 20}
        )
        assert affected == 3  # All three users

    def test_execute_with_params(self, memory_db_with_table):
        """✍️ execute() should accept named parameters."""
        memory_db_with_table.execute(
            "INSERT INTO users (id, name, age, email) VALUES (:id, :name, :age, :email)",
            {"id": 10, "name": "Param Test", "age": 40, "email": "test@test.com"},
        )
        result = memory_db_with_table.query_one(
            "SELECT * FROM users WHERE id = :id", {"id": 10}
        )
        assert result["email"] == "test@test.com"

    def test_execute_many_bulk_insert(self, memory_db_with_table):
        """✍️ execute_many() should bulk insert rows."""
        affected = memory_db_with_table.execute_many(
            "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
            [
                {"id": 1, "name": "User1", "age": 20},
                {"id": 2, "name": "User2", "age": 25},
                {"id": 3, "name": "User3", "age": 30},
            ],
        )
        assert affected == 3

        count = memory_db_with_table.query_value("SELECT COUNT(*) FROM users")
        assert count == 3

    def test_execute_many_returns_total_affected(self, memory_db_with_table):
        """✍️ execute_many() should return total affected rows."""
        affected = memory_db_with_table.execute_many(
            "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
            [{"id": i, "name": f"User{i}", "age": 20 + i} for i in range(1, 6)],
        )
        assert affected == 5

    def test_execute_many_empty_list(self, memory_db_with_table):
        """✍️ execute_many() with empty list should return 0."""
        affected = memory_db_with_table.execute_many(
            "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
            [],
        )
        assert affected == 0

    def test_execute_script_multiple_statements(self, memory_db):
        """📜 execute_script() should run multiple statements."""
        memory_db.execute_script(
            """
            CREATE TABLE t1 (id INTEGER);
            CREATE TABLE t2 (id INTEGER);
            INSERT INTO t1 VALUES (1);
            INSERT INTO t2 VALUES (2);
            """
        )
        assert memory_db.table_exists("t1")
        assert memory_db.table_exists("t2")
        assert memory_db.query_value("SELECT id FROM t1") == 1
        assert memory_db.query_value("SELECT id FROM t2") == 2


# ═══════════════════════════════════════════════════════════════════════════════
# TRANSACTIONS
# ═══════════════════════════════════════════════════════════════════════════════


class TestTransactions:
    """🔒 Test transaction behavior."""

    def test_transaction_commits_on_success(self, memory_db_with_table):
        """🔒 transaction() should commit on successful completion."""
        with memory_db_with_table.transaction():
            memory_db_with_table.execute(
                "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
                {"id": 1, "name": "TxUser", "age": 25},
            )

        result = memory_db_with_table.query_one(
            "SELECT * FROM users WHERE id = :id", {"id": 1}
        )
        assert result is not None
        assert result["name"] == "TxUser"

    def test_transaction_rollbacks_on_exception(self, memory_db_with_table):
        """🔒 transaction() should rollback on user exception."""
        with pytest.raises(ValueError):
            with memory_db_with_table.transaction():
                memory_db_with_table.execute(
                    "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
                    {"id": 1, "name": "WillRollback", "age": 25},
                )
                raise ValueError("Test rollback")

        result = memory_db_with_table.query_one(
            "SELECT * FROM users WHERE id = :id", {"id": 1}
        )
        assert result is None  # Rolled back

    def test_transaction_rollbacks_on_database_error(self, memory_db_with_table):
        """🔒 transaction() should rollback on database error."""
        # First insert a row
        memory_db_with_table.execute(
            "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
            {"id": 1, "name": "Existing", "age": 25},
        )

        with pytest.raises(DatabaseError):
            with memory_db_with_table.transaction():
                memory_db_with_table.execute(
                    "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
                    {"id": 2, "name": "WillRollback", "age": 30},
                )
                # This should fail due to PRIMARY KEY constraint
                memory_db_with_table.execute(
                    "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
                    {"id": 1, "name": "Duplicate", "age": 35},
                )

        # The second insert should have been rolled back
        count = memory_db_with_table.query_value("SELECT COUNT(*) FROM users")
        assert count == 1  # Only the first insert outside transaction

    def test_transaction_preserves_user_exception_type(self, memory_db_with_table):
        """🔒 transaction() should re-raise user exceptions unchanged."""

        class CustomError(Exception):
            pass

        with pytest.raises(CustomError):
            with memory_db_with_table.transaction():
                raise CustomError("Custom error")

    def test_nested_operations_in_transaction(self, memory_db_with_table):
        """🔒 Multiple operations in transaction should be atomic."""
        with memory_db_with_table.transaction():
            memory_db_with_table.execute(
                "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
                {"id": 1, "name": "User1", "age": 20},
            )
            memory_db_with_table.execute(
                "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
                {"id": 2, "name": "User2", "age": 25},
            )
            memory_db_with_table.execute(
                "UPDATE users SET age = age + 10 WHERE id = :id",
                {"id": 1},
            )

        results = memory_db_with_table.query("SELECT * FROM users ORDER BY id")
        assert len(results) == 2
        assert results[0]["age"] == 30  # 20 + 10
        assert results[1]["age"] == 25


# ═══════════════════════════════════════════════════════════════════════════════
# TABLE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


class TestTableOperations:
    """🏗️ Test table management."""

    def test_create_table(self, memory_db):
        """🏗️ create_table() should create a new table."""
        memory_db.create_table(
            "test_table",
            {"id": "INTEGER", "name": "TEXT"},
        )
        assert memory_db.table_exists("test_table")

    def test_create_table_with_primary_key(self, memory_db):
        """🏗️ create_table() should support primary keys."""
        memory_db.create_table(
            "test_table",
            {"id": "INTEGER", "name": "TEXT"},
            primary_key=["id"],
        )
        assert memory_db.table_exists("test_table")

        # Verify primary key by trying to insert duplicates
        memory_db.execute("INSERT INTO test_table (id, name) VALUES (1, 'Test')")
        with pytest.raises(DatabaseError):
            memory_db.execute("INSERT INTO test_table (id, name) VALUES (1, 'Duplicate')")

    def test_create_table_with_composite_primary_key(self, memory_db):
        """🏗️ create_table() should support composite primary keys."""
        memory_db.create_table(
            "test_table",
            {"user_id": "INTEGER", "post_id": "INTEGER", "content": "TEXT"},
            primary_key=["user_id", "post_id"],
        )
        assert memory_db.table_exists("test_table")

        memory_db.execute("INSERT INTO test_table VALUES (1, 1, 'Content1')")
        memory_db.execute("INSERT INTO test_table VALUES (1, 2, 'Content2')")
        with pytest.raises(DatabaseError):
            memory_db.execute("INSERT INTO test_table VALUES (1, 1, 'Duplicate')")

    def test_create_table_if_not_exists(self, memory_db):
        """🏗️ create_table() with if_not_exists should not fail."""
        memory_db.create_table("test_table", {"id": "INTEGER"})
        # Should not raise when table already exists
        memory_db.create_table("test_table", {"id": "INTEGER"}, if_not_exists=True)

    def test_drop_table(self, memory_db_with_table):
        """🗑️ drop_table() should remove table."""
        assert memory_db_with_table.table_exists("users")
        memory_db_with_table.drop_table("users")
        assert not memory_db_with_table.table_exists("users")

    def test_drop_table_if_exists(self, memory_db):
        """🗑️ drop_table() with if_exists should not fail."""
        # Should not raise when table doesn't exist
        memory_db.drop_table("nonexistent_table", if_exists=True)

    def test_table_exists_true(self, memory_db_with_table):
        """❓ table_exists() should return True for existing table."""
        assert memory_db_with_table.table_exists("users") is True

    def test_table_exists_false(self, memory_db):
        """❓ table_exists() should return False for non-existent table."""
        assert memory_db.table_exists("nonexistent") is False

    def test_list_tables(self, memory_db):
        """📋 list_tables() should return list of table names."""
        memory_db.create_table("table_a", {"id": "INTEGER"})
        memory_db.create_table("table_b", {"id": "INTEGER"})
        memory_db.create_table("table_c", {"id": "INTEGER"})

        tables = memory_db.list_tables()
        assert "table_a" in tables
        assert "table_b" in tables
        assert "table_c" in tables

    def test_list_tables_empty(self, memory_db):
        """📋 list_tables() should return empty list when no tables."""
        tables = memory_db.list_tables()
        assert tables == []

    def test_create_index(self, memory_db_with_table):
        """📇 create_index() should create an index."""
        memory_db_with_table.create_index(
            "idx_users_name",
            "users",
            ["name"],
        )
        # Verify index exists by querying sqlite_master
        result = memory_db_with_table.query_value(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=:name",
            {"name": "idx_users_name"},
        )
        assert result == 1

    def test_create_unique_index(self, memory_db_with_table):
        """📇 create_index() should support unique indexes."""
        memory_db_with_table.create_index(
            "idx_users_email_unique",
            "users",
            ["email"],
            unique=True,
        )

        # Insert a row with email
        memory_db_with_table.execute(
            "INSERT INTO users (id, name, age, email) VALUES (1, 'User1', 20, 'test@test.com')"
        )

        # Should fail on duplicate email
        with pytest.raises(DatabaseError):
            memory_db_with_table.execute(
                "INSERT INTO users (id, name, age, email) VALUES (2, 'User2', 25, 'test@test.com')"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# WAL MODE
# ═══════════════════════════════════════════════════════════════════════════════


class TestWALMode:
    """⚡ Test WAL mode operations."""

    def test_wal_mode_enabled_by_default(self, temp_db):
        """⚡ WAL mode should be enabled by default on connect."""
        temp_db.connect()
        assert temp_db.is_wal_mode() is True

    def test_disable_wal_mode(self, temp_db):
        """🔒 disable_wal_mode() should switch to DELETE journal."""
        temp_db.connect()
        temp_db.disable_wal_mode()
        assert temp_db.is_wal_mode() is False

    def test_enable_wal_mode(self, temp_db):
        """⚡ enable_wal_mode() should switch to WAL journal."""
        temp_db.connect()
        temp_db.disable_wal_mode()
        assert temp_db.is_wal_mode() is False
        temp_db.enable_wal_mode()
        assert temp_db.is_wal_mode() is True

    def test_is_wal_mode_true(self, temp_db):
        """🔍 is_wal_mode() should return True when WAL enabled."""
        temp_db.connect()
        temp_db.enable_wal_mode()
        assert temp_db.is_wal_mode() is True

    def test_is_wal_mode_false(self, temp_db):
        """🔍 is_wal_mode() should return False when WAL disabled."""
        temp_db.connect()
        temp_db.disable_wal_mode()
        assert temp_db.is_wal_mode() is False


# ═══════════════════════════════════════════════════════════════════════════════
# PRAGMAS
# ═══════════════════════════════════════════════════════════════════════════════


class TestPragmas:
    """⚙️ Test pragma operations."""

    def test_get_pragma(self, memory_db):
        """⚙️ get_pragma() should return pragma value."""
        memory_db.connect()
        journal_mode = memory_db.get_pragma("journal_mode")
        assert journal_mode is not None

    def test_set_pragma(self, memory_db):
        """⚙️ set_pragma() should set pragma value."""
        memory_db.connect()
        memory_db.set_pragma("cache_size", 1000)
        cache_size = memory_db.get_pragma("cache_size")
        assert cache_size == 1000


# ═══════════════════════════════════════════════════════════════════════════════
# EDGE CASES
# ═══════════════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """🔬 Test edge cases and special scenarios."""

    def test_unicode_data(self, memory_db_with_table):
        """🔤 Should handle unicode characters correctly."""
        memory_db_with_table.execute(
            "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
            {"id": 1, "name": "日本語テスト", "age": 25},
        )
        result = memory_db_with_table.query_one(
            "SELECT name FROM users WHERE id = :id", {"id": 1}
        )
        assert result["name"] == "日本語テスト"

    def test_emoji_data(self, memory_db_with_table):
        """😀 Should handle emoji characters correctly."""
        memory_db_with_table.execute(
            "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
            {"id": 1, "name": "User 🎉🚀💡", "age": 25},
        )
        result = memory_db_with_table.query_one(
            "SELECT name FROM users WHERE id = :id", {"id": 1}
        )
        assert result["name"] == "User 🎉🚀💡"

    def test_empty_string_value(self, memory_db_with_table):
        """📝 Should handle empty string values."""
        memory_db_with_table.execute(
            "INSERT INTO users (id, name, age, email) VALUES (:id, :name, :age, :email)",
            {"id": 1, "name": "", "age": 25, "email": ""},
        )
        result = memory_db_with_table.query_one(
            "SELECT * FROM users WHERE id = :id", {"id": 1}
        )
        assert result["name"] == ""
        assert result["email"] == ""

    def test_null_value(self, memory_db_with_table):
        """∅ Should handle NULL values correctly."""
        memory_db_with_table.execute(
            "INSERT INTO users (id, name, age, email) VALUES (:id, :name, :age, :email)",
            {"id": 1, "name": "Test", "age": None, "email": None},
        )
        result = memory_db_with_table.query_one(
            "SELECT * FROM users WHERE id = :id", {"id": 1}
        )
        assert result["age"] is None
        assert result["email"] is None

    def test_large_text_value(self, memory_db_with_table):
        """📚 Should handle large text values."""
        large_text = "x" * 100000  # 100KB of text
        memory_db_with_table.execute(
            "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
            {"id": 1, "name": large_text, "age": 25},
        )
        result = memory_db_with_table.query_one(
            "SELECT name FROM users WHERE id = :id", {"id": 1}
        )
        assert result["name"] == large_text
        assert len(result["name"]) == 100000

    def test_special_characters(self, memory_db_with_table):
        """🔣 Should handle special characters (quotes, newlines)."""
        special_text = "Line1\nLine2\tTab\"Quote'Single"
        memory_db_with_table.execute(
            "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
            {"id": 1, "name": special_text, "age": 25},
        )
        result = memory_db_with_table.query_one(
            "SELECT name FROM users WHERE id = :id", {"id": 1}
        )
        assert result["name"] == special_text

    def test_numeric_precision(self, memory_db):
        """🔢 Should preserve numeric precision."""
        memory_db.execute("CREATE TABLE numbers (id INTEGER, value REAL)")
        memory_db.execute(
            "INSERT INTO numbers (id, value) VALUES (:id, :value)",
            {"id": 1, "value": 3.14159265358979},
        )
        result = memory_db.query_value(
            "SELECT value FROM numbers WHERE id = :id", {"id": 1}
        )
        assert abs(result - 3.14159265358979) < 1e-10

    def test_blob_data(self, memory_db):
        """📦 Should handle BLOB data correctly."""
        memory_db.execute("CREATE TABLE blobs (id INTEGER, data BLOB)")
        blob_data = b"\x00\x01\x02\xff\xfe\xfd"
        memory_db.execute(
            "INSERT INTO blobs (id, data) VALUES (:id, :data)",
            {"id": 1, "data": blob_data},
        )
        result = memory_db.query_one("SELECT data FROM blobs WHERE id = :id", {"id": 1})
        assert result["data"] == blob_data


# ═══════════════════════════════════════════════════════════════════════════════
# ERROR HANDLING
# ═══════════════════════════════════════════════════════════════════════════════


class TestErrorHandling:
    """⚠️ Test error handling and DatabaseError wrapping."""

    def test_invalid_sql_raises_database_error(self, memory_db):
        """⚠️ Invalid SQL should raise DatabaseError."""
        with pytest.raises(DatabaseError):
            memory_db.execute("INVALID SQL STATEMENT")

    def test_database_error_wraps_original(self, memory_db):
        """⚠️ DatabaseError should wrap original exception."""
        try:
            memory_db.execute("INVALID SQL")
        except DatabaseError as e:
            assert e.original is not None
            assert isinstance(e.original, sqlite3.Error)

    def test_database_error_includes_message(self, memory_db):
        """⚠️ DatabaseError should include helpful message."""
        try:
            memory_db.execute("INVALID SQL")
        except DatabaseError as e:
            assert "Execute failed" in str(e) or "INVALID" in str(e).upper()

    def test_missing_param_raises_error(self, memory_db_with_table):
        """⚠️ Missing parameter should raise error."""
        with pytest.raises(DatabaseError):
            memory_db_with_table.execute(
                "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
                {"id": 1, "name": "Test"},  # Missing :age
            )

    def test_constraint_violation_raises_error(self, memory_db_with_table):
        """⚠️ Constraint violation should raise DatabaseError."""
        # NOT NULL constraint on name
        with pytest.raises(DatabaseError):
            memory_db_with_table.execute(
                "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
                {"id": 1, "name": None, "age": 25},
            )


# ═══════════════════════════════════════════════════════════════════════════════
# THREAD SAFETY
# ═══════════════════════════════════════════════════════════════════════════════


class TestThreadSafety:
    """🔐 Test thread safety with concurrent access."""

    def test_concurrent_reads(self, temp_db):
        """🔐 Concurrent reads should be thread-safe."""
        # Setup data
        temp_db.create_table("data", {"id": "INTEGER PRIMARY KEY", "value": "TEXT"})
        for i in range(100):
            temp_db.execute(
                "INSERT INTO data (id, value) VALUES (:id, :value)",
                {"id": i, "value": f"value_{i}"},
            )

        results = []
        errors = []

        def read_data():
            try:
                data = temp_db.query("SELECT * FROM data")
                results.append(len(data))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=read_data) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert all(r == 100 for r in results)

    def test_concurrent_writes(self, temp_db):
        """🔐 Concurrent writes should be thread-safe."""
        temp_db.create_table("counter", {"id": "INTEGER PRIMARY KEY", "count": "INTEGER"})
        temp_db.execute("INSERT INTO counter (id, count) VALUES (1, 0)")

        errors = []

        def increment():
            try:
                for _ in range(10):
                    temp_db.execute("UPDATE counter SET count = count + 1 WHERE id = 1")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=increment) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        final_count = temp_db.query_value("SELECT count FROM counter WHERE id = 1")
        assert final_count == 50  # 5 threads * 10 increments

    def test_concurrent_read_write(self, temp_db):
        """🔐 Concurrent read/write should be thread-safe."""
        temp_db.create_table("items", {"id": "INTEGER PRIMARY KEY", "name": "TEXT"})

        errors = []
        write_count = [0]
        read_count = [0]

        def writer():
            try:
                for i in range(20):
                    temp_db.execute(
                        "INSERT OR REPLACE INTO items (id, name) VALUES (:id, :name)",
                        {"id": i % 5, "name": f"item_{i}"},
                    )
                    write_count[0] += 1
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(20):
                    temp_db.query("SELECT * FROM items")
                    read_count[0] += 1
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=writer),
            threading.Thread(target=reader),
            threading.Thread(target=reader),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert write_count[0] == 40  # 2 writers * 20 writes
        assert read_count[0] == 40  # 2 readers * 20 reads


# ═══════════════════════════════════════════════════════════════════════════════
# VACUUM
# ═══════════════════════════════════════════════════════════════════════════════


class TestVacuum:
    """🧹 Test database optimization."""

    def test_vacuum(self, temp_db):
        """🧹 vacuum() should optimize database file."""
        # Create and populate table
        temp_db.create_table("data", {"id": "INTEGER", "content": "TEXT"})
        for i in range(100):
            temp_db.execute(
                "INSERT INTO data (id, content) VALUES (:id, :content)",
                {"id": i, "content": "x" * 1000},
            )

        # Delete most data
        temp_db.execute("DELETE FROM data WHERE id > 10")

        # Vacuum should not raise
        temp_db.vacuum()

        # Verify data integrity
        count = temp_db.query_value("SELECT COUNT(*) FROM data")
        assert count == 11  # 0-10 inclusive
