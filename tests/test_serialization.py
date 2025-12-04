"""🧪 Tests for scriptman.serialization module."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path, PurePath
from uuid import UUID

from pydantic import BaseModel

from scriptman.serialization import serialize

# ═══════════════════════════════════════════════════════════════════════════════
# TEST MODELS
# ═══════════════════════════════════════════════════════════════════════════════


class Color(Enum):
    """Test enum."""

    RED = "red"
    GREEN = "green"
    BLUE = "blue"


class SampleModel(BaseModel):
    """Test Pydantic model."""

    name: str
    value: int


# ═══════════════════════════════════════════════════════════════════════════════
# BASIC TYPE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestPrimitives:
    """Test primitive types pass through unchanged."""

    def test_none_returns_none(self):
        """🔄 None should return None."""
        assert serialize(None) is None

    def test_string_passes_through(self):
        """🔄 Strings should pass through unchanged."""
        assert serialize("hello") == "hello"

    def test_int_passes_through(self):
        """🔄 Integers should pass through unchanged."""
        assert serialize(42) == 42

    def test_float_passes_through(self):
        """🔄 Floats should pass through unchanged."""
        assert serialize(3.14) == 3.14

    def test_bool_passes_through(self):
        """🔄 Booleans should pass through unchanged."""
        assert serialize(True) is True
        assert serialize(False) is False


class TestPathSerialization:
    """Test Path types convert to strings."""

    def test_path_to_string(self):
        """📁 Path should convert to string."""
        assert serialize(Path(".logs")) == ".logs"

    def test_pure_path_to_string(self):
        """📁 PurePath should convert to string."""
        assert serialize(PurePath(".logs")) == ".logs"

    def test_nested_path_to_string(self):
        """📁 Nested paths should convert to string."""
        result = serialize(Path("data/cache/test.db"))
        # Normalize for cross-platform (Windows uses backslashes)
        assert result.replace("\\", "/") == "data/cache/test.db"


class TestDateTimeSerialization:
    """Test datetime types convert to ISO format."""

    def test_datetime_to_iso(self):
        """⏱️ datetime should convert to ISO string."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        assert serialize(dt) == "2024-01-15T10:30:00"

    def test_datetime_with_microseconds(self):
        """⏱️ datetime with microseconds should include them."""
        dt = datetime(2024, 1, 15, 10, 30, 0, 123456)
        assert serialize(dt) == "2024-01-15T10:30:00.123456"

    def test_date_to_iso(self):
        """⏱️ date should convert to ISO string."""
        d = date(2024, 1, 15)
        assert serialize(d) == "2024-01-15"

    def test_time_to_iso(self):
        """⏱️ time should convert to ISO string."""
        t = time(10, 30, 0)
        assert serialize(t) == "10:30:00"

    def test_time_with_microseconds(self):
        """⏱️ time with microseconds should include them."""
        t = time(10, 30, 0, 123456)
        assert serialize(t) == "10:30:00.123456"


class TestEnumSerialization:
    """Test Enum types convert to their values."""

    def test_enum_to_value_string(self):
        """🏷️ Enum with string value should convert to string."""
        assert serialize(Color.RED) == "red"
        assert serialize(Color.GREEN) == "green"

    def test_enum_to_value_int(self):
        """🏷️ Enum with int value should convert to int."""

        class Priority(Enum):
            LOW = 1
            HIGH = 10

        assert serialize(Priority.LOW) == 1
        assert serialize(Priority.HIGH) == 10


class TestUUIDSerialization:
    """Test UUID converts to string."""

    def test_uuid_to_string(self):
        """🔑 UUID should convert to string."""
        uid = UUID("12345678-1234-5678-1234-567812345678")
        assert serialize(uid) == "12345678-1234-5678-1234-567812345678"

    def test_uuid_lowercase(self):
        """🔑 UUID string should be lowercase."""
        uid = UUID("ABCDEF12-1234-5678-1234-567812345678")
        result = serialize(uid)
        assert result == result.lower()


class TestDecimalSerialization:
    """Test Decimal converts to string (preserves precision)."""

    def test_decimal_to_string(self):
        """💰 Decimal should convert to string."""
        assert serialize(Decimal("3.14159")) == "3.14159"

    def test_decimal_preserves_precision(self):
        """💰 Decimal precision should be preserved."""
        # Note: Python's str(Decimal) uses scientific notation for small numbers
        assert serialize(Decimal("0.0000001")) == "1E-7"
        # But explicit decimal notation is preserved
        assert serialize(Decimal("0.123456789")) == "0.123456789"

    def test_decimal_large_number(self):
        """💰 Large Decimal should preserve all digits."""
        big = Decimal("12345678901234567890.12345678901234567890")
        assert serialize(big) == "12345678901234567890.12345678901234567890"

    def test_decimal_negative(self):
        """💰 Negative Decimal should preserve sign."""
        assert serialize(Decimal("-42.5")) == "-42.5"


# ═══════════════════════════════════════════════════════════════════════════════
# PYDANTIC MODEL TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestPydanticSerialization:
    """Test Pydantic models serialize via model_dump()."""

    def test_pydantic_model_to_dict(self):
        """📦 Pydantic model should convert to dict."""
        model = SampleModel(name="test", value=42)
        result = serialize(model)
        assert result == {"name": "test", "value": 42}

    def test_nested_pydantic_with_special_types(self):
        """📦 Pydantic model with special types should serialize recursively."""

        class ComplexModel(BaseModel):
            path: Path
            created: datetime

        model = ComplexModel(
            path=Path(".logs"),
            created=datetime(2024, 1, 15, 10, 30, 0),
        )
        result = serialize(model)
        assert result == {
            "path": ".logs",
            "created": "2024-01-15T10:30:00",
        }

    def test_pydantic_with_nested_model(self):
        """📦 Nested Pydantic models should serialize recursively."""

        class Inner(BaseModel):
            value: int

        class Outer(BaseModel):
            inner: Inner
            name: str

        model = Outer(inner=Inner(value=42), name="test")
        result = serialize(model)
        assert result == {"inner": {"value": 42}, "name": "test"}


# ═══════════════════════════════════════════════════════════════════════════════
# NESTED STRUCTURE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestNestedStructures:
    """Test nested dicts, lists, tuples, sets."""

    def test_dict_serializes_recursively(self):
        """📦 Dict should serialize values recursively."""
        data = {
            "path": Path(".logs"),
            "count": 42,
            "name": "test",
        }
        result = serialize(data)
        assert result == {
            "path": ".logs",
            "count": 42,
            "name": "test",
        }

    def test_list_serializes_recursively(self):
        """📦 List should serialize items recursively."""
        data = [Path(".logs"), datetime(2024, 1, 15), "text"]
        result = serialize(data)
        assert result == [".logs", "2024-01-15T00:00:00", "text"]

    def test_tuple_becomes_list(self):
        """📦 Tuple should convert to list."""
        data = (Path(".logs"), 42)
        result = serialize(data)
        assert result == [".logs", 42]
        assert isinstance(result, list)

    def test_set_becomes_list(self):
        """📦 Set should convert to list."""
        data = {1, 2, 3}
        result = serialize(data)
        assert sorted(result) == [1, 2, 3]
        assert isinstance(result, list)

    def test_frozenset_becomes_list(self):
        """📦 Frozenset should convert to list."""
        data = frozenset([1, 2, 3])
        result = serialize(data)
        assert sorted(result) == [1, 2, 3]
        assert isinstance(result, list)

    def test_deeply_nested_structure(self):
        """📦 Deeply nested structures should serialize correctly."""
        data = {
            "config": {
                "paths": [Path(".logs"), Path(".cache")],
                "settings": {
                    "timeout": Decimal("30.5"),
                    "color": Color.BLUE,
                },
            },
        }
        result = serialize(data)
        assert result == {
            "config": {
                "paths": [".logs", ".cache"],
                "settings": {
                    "timeout": "30.5",
                    "color": "blue",
                },
            },
        }

    def test_dict_with_various_key_types(self):
        """📦 Dict keys should remain unchanged."""
        data = {"string_key": 1, "another": 2}
        result = serialize(data)
        assert result == {"string_key": 1, "another": 2}


# ═══════════════════════════════════════════════════════════════════════════════
# EDGE CASE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """Test edge cases and unusual inputs."""

    def test_empty_dict(self):
        """📦 Empty dict should return empty dict."""
        assert serialize({}) == {}

    def test_empty_list(self):
        """📦 Empty list should return empty list."""
        assert serialize([]) == []

    def test_empty_string(self):
        """📦 Empty string should return empty string."""
        assert serialize("") == ""

    def test_unicode_string(self):
        """🌍 Unicode strings should pass through."""
        assert serialize("你好世界") == "你好世界"
        assert serialize("🚀🔄✅") == "🚀🔄✅"

    def test_special_characters_in_string(self):
        """🔤 Special characters should pass through."""
        assert serialize("hello\nworld\ttab") == "hello\nworld\ttab"

    def test_curly_braces_in_string(self):
        """🔤 Curly braces should pass through."""
        assert serialize("{key}") == "{key}"

    def test_unknown_type_passes_through(self):
        """⚠️ Unknown types should pass through unchanged."""

        class CustomClass:
            pass

        obj = CustomClass()
        result = serialize(obj)
        assert result is obj  # Same object reference

    def test_bytes_passes_through(self):
        """⚠️ Bytes should pass through (not serializable)."""
        data = b"hello"
        result = serialize(data)
        assert result == b"hello"

    def test_callable_passes_through(self):
        """⚠️ Callables should pass through unchanged."""

        def my_func():
            pass

        result = serialize(my_func)
        assert result is my_func

    def test_class_passes_through(self):
        """⚠️ Class objects should pass through unchanged."""

        class MyClass:
            pass

        result = serialize(MyClass)
        assert result is MyClass

    def test_zero_values(self):
        """🔢 Zero values should serialize correctly."""
        assert serialize(0) == 0
        assert serialize(0.0) == 0.0
        assert serialize(Decimal("0")) == "0"

    def test_negative_numbers(self):
        """🔢 Negative numbers should pass through."""
        assert serialize(-42) == -42
        assert serialize(-3.14) == -3.14

    def test_very_long_string(self):
        """📏 Very long strings should pass through."""
        long_string = "x" * 10000
        assert serialize(long_string) == long_string

    def test_deeply_nested_list(self):
        """📏 Deeply nested lists should serialize."""
        data = [[[[[Path(".logs")]]]]]
        result = serialize(data)
        assert result == [[[[[".logs"]]]]]

    def test_mixed_collection_types(self):
        """📦 Mixed collection types should all become lists."""
        data = {
            "list": [1, 2],
            "tuple": (3, 4),
            "set": {5, 6},
        }
        result = serialize(data)
        assert result["list"] == [1, 2]
        assert result["tuple"] == [3, 4]
        assert sorted(result["set"]) == [5, 6]


# ═══════════════════════════════════════════════════════════════════════════════
# INTEGRATION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestIntegration:
    """Test realistic usage scenarios."""

    def test_config_like_structure(self):
        """⚙️ Config-like data should serialize for TOML/JSON."""
        config = {
            "data": {
                "dir": Path(".data"),
                "logs": Path(".data/logs"),
            },
            "logging": {
                "level": "DEBUG",
                "format": "simple",
            },
            "execution": {
                "concurrent": True,
                "timeout": Decimal("30.0"),
            },
        }
        result = serialize(config)

        # All paths should be strings
        assert result["data"]["dir"] == ".data"
        assert result["data"]["logs"].replace("\\", "/") == ".data/logs"

        # Primitives unchanged
        assert result["logging"]["level"] == "DEBUG"
        assert result["execution"]["concurrent"] is True

        # Decimal to string
        assert result["execution"]["timeout"] == "30.0"

    def test_event_like_structure(self):
        """📡 Event-like data should serialize for storage."""
        event = {
            "id": UUID("12345678-1234-5678-1234-567812345678"),
            "timestamp": datetime(2024, 1, 15, 10, 30, 0),
            "type": "LOG",
            "data": {
                "path": Path(".logs/app.log"),
                "size": 1024,
            },
        }
        result = serialize(event)

        assert result["id"] == "12345678-1234-5678-1234-567812345678"
        assert result["timestamp"] == "2024-01-15T10:30:00"
        assert result["type"] == "LOG"
        assert result["data"]["size"] == 1024

    def test_api_response_like_structure(self):
        """📡 API response data should serialize correctly."""

        class User(BaseModel):
            id: int
            name: str
            created_at: datetime

        class Response(BaseModel):
            users: list[User]
            total: int

        response = Response(
            users=[
                User(id=1, name="Alice", created_at=datetime(2024, 1, 1)),
                User(id=2, name="Bob", created_at=datetime(2024, 1, 2)),
            ],
            total=2,
        )

        result = serialize(response)

        assert result["total"] == 2
        assert len(result["users"]) == 2
        assert result["users"][0]["name"] == "Alice"
        assert result["users"][0]["created_at"] == "2024-01-01T00:00:00"
