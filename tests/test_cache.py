"""Tests for scriptman.cache — SQLite-backed caching.

Coverage Goals:
- Core operations (get, set, delete, exists, clear)
- Bulk operations (get_many, set_many, delete_many)
- Tags (invalidate_tag, get_by_tag)
- TTL expiration (lazy + active)
- Decorator (sync and async)
- Statistics (hits, misses, hit_ratio)
- LRU eviction
- Cleanup expired
- Sharded backend
- Edge cases (unicode, empty, large values)
- Stampede prevention

Target: 95%+ coverage
"""

from __future__ import annotations

import asyncio
import time

import pytest

from scriptman.cache import (
    Cache,
    CacheBackend,
    CacheStats,
    ShardedSQLiteBackend,
    SQLiteCacheBackend,
    cache,
)
from scriptman.cache.backends import (
    CacheEntry,
    get_backend,
    list_backends,
    register_backend,
    unregister_backend,
)

# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture
def temp_cache(tmp_path):
    """📦 Temporary SQLite cache backend for isolated tests."""
    db_path = tmp_path / "test_cache.db"
    backend = SQLiteCacheBackend(db_path)
    yield backend
    backend.close()


@pytest.fixture
def temp_sharded_cache(tmp_path):
    """📦 Temporary sharded cache backend for isolated tests."""
    backend = ShardedSQLiteBackend(tmp_path, shards=4)
    yield backend
    backend.close()


@pytest.fixture
def cache_with_data(temp_cache):
    """📦 Cache backend with pre-populated test data."""
    temp_cache.set("user:1", {"name": "Alice", "age": 30}, tags=["users"])
    temp_cache.set("user:2", {"name": "Bob", "age": 25}, tags=["users"])
    temp_cache.set("product:1", {"name": "Widget", "price": 9.99}, tags=["products"])
    yield temp_cache


@pytest.fixture(autouse=True)
def reset_cache_singleton():
    """🔄 Reset the cache singleton between tests."""
    # Store original backend
    original_backend = cache._backend

    yield

    # Restore/reset after test
    if cache._backend is not None and cache._backend is not original_backend:
        cache._backend.close()
    cache._backend = original_backend


# ═══════════════════════════════════════════════════════════════════════════════
# CORE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


class TestCoreOperations:
    """🔍 Test basic get/set/delete operations."""

    def test_set_and_get(self, temp_cache):
        """✍️ set() then get() should return the stored value."""
        temp_cache.set("key", "value")
        assert temp_cache.get("key") == "value"

    def test_get_nonexistent_returns_none(self, temp_cache):
        """🔍 get() on missing key should return None."""
        assert temp_cache.get("nonexistent") is None

    def test_set_overwrites_existing(self, temp_cache):
        """✍️ set() should overwrite existing value."""
        temp_cache.set("key", "value1")
        temp_cache.set("key", "value2")
        assert temp_cache.get("key") == "value2"

    def test_delete_removes_key(self, temp_cache):
        """🧹 delete() should remove the key."""
        temp_cache.set("key", "value")
        assert temp_cache.delete("key") is True
        assert temp_cache.get("key") is None

    def test_delete_nonexistent_returns_false(self, temp_cache):
        """🧹 delete() on missing key should return False."""
        assert temp_cache.delete("nonexistent") is False

    def test_exists_true_for_existing_key(self, temp_cache):
        """🔍 exists() should return True for existing key."""
        temp_cache.set("key", "value")
        assert temp_cache.exists("key") is True

    def test_exists_false_for_missing_key(self, temp_cache):
        """🔍 exists() should return False for missing key."""
        assert temp_cache.exists("nonexistent") is False

    def test_clear_removes_all_keys(self, cache_with_data):
        """🧹 clear() should remove all cached values."""
        count = cache_with_data.clear()
        assert count == 3
        assert cache_with_data.get("user:1") is None
        assert cache_with_data.get("user:2") is None
        assert cache_with_data.get("product:1") is None

    def test_set_returns_true(self, temp_cache):
        """✍️ set() should return True on success."""
        result = temp_cache.set("key", "value")
        assert result is True


# ═══════════════════════════════════════════════════════════════════════════════
# DATA TYPES
# ═══════════════════════════════════════════════════════════════════════════════


class TestDataTypes:
    """📦 Test caching various data types."""

    def test_string_value(self, temp_cache):
        """📝 Should cache string values."""
        temp_cache.set("str", "hello world")
        assert temp_cache.get("str") == "hello world"

    def test_integer_value(self, temp_cache):
        """🔢 Should cache integer values."""
        temp_cache.set("int", 42)
        assert temp_cache.get("int") == 42

    def test_float_value(self, temp_cache):
        """🔢 Should cache float values."""
        temp_cache.set("float", 3.14159)
        assert abs(temp_cache.get("float") - 3.14159) < 1e-10

    def test_boolean_value(self, temp_cache):
        """✅ Should cache boolean values."""
        temp_cache.set("bool_true", True)
        temp_cache.set("bool_false", False)
        assert temp_cache.get("bool_true") is True
        assert temp_cache.get("bool_false") is False

    def test_none_value(self, temp_cache):
        """∅ Should cache None values (but get returns None for missing too)."""
        temp_cache.set("none", None)
        # Note: This is ambiguous - can't distinguish None value from missing key
        # Use exists() to check if key exists
        assert temp_cache.exists("none") is True

    def test_list_value(self, temp_cache):
        """📋 Should cache list values."""
        temp_cache.set("list", [1, 2, 3, "four", {"five": 5}])
        result = temp_cache.get("list")
        assert result == [1, 2, 3, "four", {"five": 5}]

    def test_dict_value(self, temp_cache):
        """📖 Should cache dict values."""
        data = {"name": "Test", "nested": {"a": 1, "b": 2}}
        temp_cache.set("dict", data)
        assert temp_cache.get("dict") == data

    def test_nested_structure(self, temp_cache):
        """🏗️ Should cache deeply nested structures."""
        data = {
            "users": [
                {"id": 1, "name": "Alice", "tags": ["admin", "user"]},
                {"id": 2, "name": "Bob", "tags": ["user"]},
            ],
            "metadata": {"version": "1.0", "count": 2},
        }
        temp_cache.set("nested", data)
        assert temp_cache.get("nested") == data


# ═══════════════════════════════════════════════════════════════════════════════
# TTL EXPIRATION
# ═══════════════════════════════════════════════════════════════════════════════


class TestTTLExpiration:
    """⏱️ Test TTL-based expiration."""

    def test_ttl_expires_value(self, temp_cache):
        """⏱️ Value should expire after TTL seconds."""
        temp_cache.set("expiring", "value", ttl=1)
        assert temp_cache.get("expiring") == "value"

        # Wait for expiration
        time.sleep(1.1)
        assert temp_cache.get("expiring") is None

    def test_ttl_none_never_expires(self, temp_cache):
        """⏱️ TTL=None should never expire."""
        temp_cache.set("permanent", "value", ttl=None)
        assert temp_cache.get("permanent") == "value"
        # Can't really test "never" but we can verify it exists
        assert temp_cache.exists("permanent") is True

    def test_exists_false_for_expired(self, temp_cache):
        """🔍 exists() should return False for expired key."""
        temp_cache.set("expiring", "value", ttl=1)
        time.sleep(1.1)
        assert temp_cache.exists("expiring") is False

    def test_cleanup_expired_removes_old_entries(self, temp_cache):
        """🧹 cleanup_expired() should remove expired entries."""
        temp_cache.set("keep", "value1", ttl=10)
        temp_cache.set("expire1", "value2", ttl=1)
        temp_cache.set("expire2", "value3", ttl=1)

        time.sleep(1.1)

        removed = temp_cache.cleanup_expired()
        assert removed == 2
        assert temp_cache.get("keep") == "value1"
        assert temp_cache.get("expire1") is None
        assert temp_cache.get("expire2") is None


# ═══════════════════════════════════════════════════════════════════════════════
# BULK OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


class TestBulkOperations:
    """📦 Test bulk get/set/delete operations."""

    def test_set_many(self, temp_cache):
        """✍️ set_many() should store multiple values."""
        items = {"a": 1, "b": 2, "c": 3}
        count = temp_cache.set_many(items)
        assert count == 3
        assert temp_cache.get("a") == 1
        assert temp_cache.get("b") == 2
        assert temp_cache.get("c") == 3

    def test_set_many_empty_dict(self, temp_cache):
        """✍️ set_many() with empty dict should return 0."""
        count = temp_cache.set_many({})
        assert count == 0

    def test_get_many(self, cache_with_data):
        """🔍 get_many() should return dict of found values."""
        result = cache_with_data.get_many(["user:1", "user:2", "nonexistent"])
        assert "user:1" in result
        assert "user:2" in result
        assert "nonexistent" not in result
        assert result["user:1"]["name"] == "Alice"

    def test_get_many_empty_list(self, temp_cache):
        """🔍 get_many() with empty list should return empty dict."""
        result = temp_cache.get_many([])
        assert result == {}

    def test_delete_many(self, cache_with_data):
        """🧹 delete_many() should remove multiple keys."""
        count = cache_with_data.delete_many(["user:1", "user:2"])
        assert count == 2
        assert cache_with_data.get("user:1") is None
        assert cache_with_data.get("user:2") is None
        assert cache_with_data.get("product:1") is not None  # Not deleted

    def test_delete_many_empty_list(self, temp_cache):
        """🧹 delete_many() with empty list should return 0."""
        count = temp_cache.delete_many([])
        assert count == 0


# ═══════════════════════════════════════════════════════════════════════════════
# TAG OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


class TestTagOperations:
    """🏷️ Test tag-based grouping and invalidation."""

    def test_set_with_tags(self, temp_cache):
        """🏷️ set() should associate tags with value."""
        temp_cache.set("item", "value", tags=["tag1", "tag2"])
        assert temp_cache.get("item") == "value"

    def test_get_by_tag(self, cache_with_data):
        """🔍 get_by_tag() should return all values with tag."""
        users = cache_with_data.get_by_tag("users")
        assert len(users) == 2
        assert "user:1" in users
        assert "user:2" in users

    def test_get_by_tag_empty(self, temp_cache):
        """🔍 get_by_tag() should return empty dict for unknown tag."""
        result = temp_cache.get_by_tag("nonexistent_tag")
        assert result == {}

    def test_invalidate_tag(self, cache_with_data):
        """🧹 invalidate_tag() should delete all values with tag."""
        count = cache_with_data.invalidate_tag("users")
        assert count == 2
        assert cache_with_data.get("user:1") is None
        assert cache_with_data.get("user:2") is None
        assert cache_with_data.get("product:1") is not None  # Different tag

    def test_invalidate_tag_nonexistent(self, temp_cache):
        """🧹 invalidate_tag() should return 0 for unknown tag."""
        count = temp_cache.invalidate_tag("nonexistent_tag")
        assert count == 0

    def test_multiple_tags_per_item(self, temp_cache):
        """🏷️ Item can have multiple tags."""
        temp_cache.set("item", "value", tags=["tag1", "tag2", "tag3"])

        # Should appear in all three tag queries
        assert "item" in temp_cache.get_by_tag("tag1")
        assert "item" in temp_cache.get_by_tag("tag2")
        assert "item" in temp_cache.get_by_tag("tag3")

    def test_update_replaces_tags(self, temp_cache):
        """🏷️ Updating with new tags should replace old tags."""
        temp_cache.set("item", "v1", tags=["old_tag"])
        temp_cache.set("item", "v2", tags=["new_tag"])

        assert temp_cache.get_by_tag("old_tag") == {}
        assert "item" in temp_cache.get_by_tag("new_tag")


# ═══════════════════════════════════════════════════════════════════════════════
# STATISTICS
# ═══════════════════════════════════════════════════════════════════════════════


class TestStatistics:
    """📊 Test cache statistics tracking."""

    def test_stats_returns_cache_stats(self, temp_cache):
        """📊 stats() should return CacheStats instance."""
        stats = temp_cache.stats()
        assert isinstance(stats, CacheStats)

    def test_stats_hits_and_misses(self, temp_cache):
        """📊 stats should track hits and misses."""
        temp_cache.set("key", "value")

        # Generate hits
        temp_cache.get("key")
        temp_cache.get("key")

        # Generate misses
        temp_cache.get("nonexistent")

        stats = temp_cache.stats()
        assert stats.hits == 2
        assert stats.misses == 1

    def test_stats_hit_ratio(self, temp_cache):
        """📊 hit_ratio should calculate correctly."""
        temp_cache.set("key", "value")
        temp_cache.get("key")  # hit
        temp_cache.get("key")  # hit
        temp_cache.get("missing")  # miss

        stats = temp_cache.stats()
        assert abs(stats.hit_ratio - 2 / 3) < 0.01

    def test_stats_hit_ratio_zero_when_empty(self, temp_cache):
        """📊 hit_ratio should be 0.0 when no operations."""
        stats = temp_cache.stats()
        assert stats.hit_ratio == 0.0

    def test_stats_size_bytes(self, temp_cache):
        """📊 size_bytes should track total cache size."""
        temp_cache.set("key", "x" * 1000)
        stats = temp_cache.stats()
        assert stats.size_bytes > 0

    def test_stats_key_count(self, cache_with_data):
        """📊 key_count should track number of keys."""
        stats = cache_with_data.stats()
        assert stats.key_count == 3


# ═══════════════════════════════════════════════════════════════════════════════
# LRU EVICTION
# ═══════════════════════════════════════════════════════════════════════════════


class TestLRUEviction:
    """🧹 Test least-recently-used eviction."""

    def test_evict_lru_removes_oldest(self, temp_cache):
        """🧹 evict_lru() should remove least recently accessed."""
        # Insert items
        temp_cache.set("a", 1)
        time.sleep(0.01)
        temp_cache.set("b", 2)
        time.sleep(0.01)
        temp_cache.set("c", 3)

        # Access 'a' to make it recently used
        temp_cache.get("a")

        # Evict 1 item - should be 'b' (least recently accessed)
        evicted = temp_cache.evict_lru(1)
        assert evicted == 1
        assert temp_cache.get("a") is not None  # Recently accessed
        assert temp_cache.get("c") is not None  # More recent than b
        # b should be evicted (inserted after a, never accessed after)

    def test_evict_lru_respects_count(self, temp_cache):
        """🧹 evict_lru() should evict exactly count items."""
        for i in range(10):
            temp_cache.set(f"key{i}", i)

        evicted = temp_cache.evict_lru(5)
        assert evicted == 5

        stats = temp_cache.stats()
        assert stats.key_count == 5


# ═══════════════════════════════════════════════════════════════════════════════
# STAMPEDE PREVENTION
# ═══════════════════════════════════════════════════════════════════════════════


class TestStampedePrevention:
    """🔐 Test stampede prevention with get_or_set."""

    def test_get_or_set_returns_cached(self, temp_cache):
        """🔐 get_or_set() should return cached value if exists."""
        temp_cache.set("key", "existing")

        call_count = [0]

        def factory():
            call_count[0] += 1
            return "computed"

        result = temp_cache.get_or_set("key", factory)
        assert result == "existing"
        assert call_count[0] == 0  # Factory not called

    def test_get_or_set_computes_missing(self, temp_cache):
        """🔐 get_or_set() should compute and cache if missing."""
        call_count = [0]

        def factory():
            call_count[0] += 1
            return "computed"

        result = temp_cache.get_or_set("new_key", factory)
        assert result == "computed"
        assert call_count[0] == 1

        # Second call should use cached value
        result2 = temp_cache.get_or_set("new_key", factory)
        assert result2 == "computed"
        assert call_count[0] == 1  # Factory not called again

    def test_get_or_set_with_ttl(self, temp_cache):
        """🔐 get_or_set() should respect TTL."""
        result = temp_cache.get_or_set("key", lambda: "value", ttl=1)
        assert result == "value"

        time.sleep(1.1)
        # Should recompute after expiry
        result2 = temp_cache.get_or_set("key", lambda: "new_value", ttl=1)
        assert result2 == "new_value"

    def test_get_or_set_with_tags(self, temp_cache):
        """🔐 get_or_set() should set tags."""
        temp_cache.get_or_set("key", lambda: "value", tags=["my_tag"])
        assert "key" in temp_cache.get_by_tag("my_tag")


# ═══════════════════════════════════════════════════════════════════════════════
# DECORATOR - SYNC
# ═══════════════════════════════════════════════════════════════════════════════


class TestDecoratorSync:
    """🎀 Test @cache.result() decorator with sync functions."""

    def test_decorator_caches_result(self, tmp_path):
        """🎀 Decorated function should cache return value."""
        # Create isolated backend
        backend = SQLiteCacheBackend(tmp_path / "decorator_test.db")
        cache.use_backend(backend)

        call_count = [0]

        @cache.result(ttl=60)
        def expensive_func(x: int) -> int:
            call_count[0] += 1
            return x * 2

        # First call computes
        result1 = expensive_func(5)
        assert result1 == 10
        assert call_count[0] == 1

        # Second call uses cache
        result2 = expensive_func(5)
        assert result2 == 10
        assert call_count[0] == 1  # Not incremented

        # Different arg computes again
        result3 = expensive_func(10)
        assert result3 == 20
        assert call_count[0] == 2

    def test_decorator_with_tags(self, tmp_path):
        """🏷️ Decorated function should support tags."""
        backend = SQLiteCacheBackend(tmp_path / "decorator_tags.db")
        cache.use_backend(backend)

        @cache.result(ttl=60, tags=["functions"])
        def tagged_func(x: int) -> int:
            return x + 1

        tagged_func(1)
        tagged_func(2)

        # Clear by tag
        count = cache.invalidate_tag("functions")
        assert count >= 2

    def test_decorator_with_custom_key_fn(self, tmp_path):
        """🔑 Decorated function should support custom key function."""
        backend = SQLiteCacheBackend(tmp_path / "custom_key.db")
        cache.use_backend(backend)

        call_count = [0]

        @cache.result(ttl=60, key_fn=lambda x, y: f"sum:{x}:{y}")
        def add(x: int, y: int) -> int:
            call_count[0] += 1
            return x + y

        result1 = add(1, 2)
        assert result1 == 3
        assert call_count[0] == 1

        # Same args, same key
        result2 = add(1, 2)
        assert result2 == 3
        assert call_count[0] == 1

    def test_decorator_preserves_function_name(self, tmp_path):
        """📝 Decorated function should preserve __name__."""
        backend = SQLiteCacheBackend(tmp_path / "funcname.db")
        cache.use_backend(backend)

        @cache.result(ttl=60)
        def my_function():
            return 42

        assert my_function.__name__ == "my_function"


# ═══════════════════════════════════════════════════════════════════════════════
# DECORATOR - ASYNC
# ═══════════════════════════════════════════════════════════════════════════════


class TestDecoratorAsync:
    """🎀 Test @cache.result() decorator with async functions."""

    def test_async_decorator_caches_result(self, tmp_path):
        """🎀 Async decorated function should cache return value."""
        backend = SQLiteCacheBackend(tmp_path / "async_decorator.db")
        cache.use_backend(backend)

        call_count = [0]

        @cache.result(ttl=60)
        async def async_expensive(x: int) -> int:
            call_count[0] += 1
            await asyncio.sleep(0.01)
            return x * 3

        # First call computes
        result1 = asyncio.run(async_expensive(5))
        assert result1 == 15
        assert call_count[0] == 1

        # Second call uses cache
        result2 = asyncio.run(async_expensive(5))
        assert result2 == 15
        assert call_count[0] == 1

    def test_async_decorator_with_tags(self, tmp_path):
        """🏷️ Async decorated function should support tags."""
        backend = SQLiteCacheBackend(tmp_path / "async_tags.db")
        cache.use_backend(backend)

        @cache.result(ttl=60, tags=["async_funcs"])
        async def async_tagged(x: int) -> int:
            return x * 2

        asyncio.run(async_tagged(1))
        asyncio.run(async_tagged(2))

        count = cache.invalidate_tag("async_funcs")
        assert count >= 2

    def test_async_function_stays_async(self, tmp_path):
        """🔄 Decorated async function should remain awaitable."""
        backend = SQLiteCacheBackend(tmp_path / "stays_async.db")
        cache.use_backend(backend)

        @cache.result(ttl=60)
        async def async_func() -> str:
            return "async"

        # Should be awaitable
        import inspect

        assert inspect.iscoroutinefunction(async_func)


# ═══════════════════════════════════════════════════════════════════════════════
# SHARDED BACKEND
# ═══════════════════════════════════════════════════════════════════════════════


class TestShardedBackend:
    """💾 Test sharded SQLite backend."""

    def test_sharded_set_and_get(self, temp_sharded_cache):
        """✍️ Sharded cache should store and retrieve values."""
        temp_sharded_cache.set("key", "value")
        assert temp_sharded_cache.get("key") == "value"

    def test_sharded_distributes_keys(self, temp_sharded_cache):
        """🔀 Keys should be distributed across shards."""
        # Insert many keys
        for i in range(100):
            temp_sharded_cache.set(f"key_{i}", i)

        # All should be retrievable
        for i in range(100):
            assert temp_sharded_cache.get(f"key_{i}") == i

    def test_sharded_tags_work(self, temp_sharded_cache):
        """🏷️ Tags should work across shards."""
        temp_sharded_cache.set("a", 1, tags=["shared_tag"])
        temp_sharded_cache.set("b", 2, tags=["shared_tag"])
        temp_sharded_cache.set("c", 3, tags=["shared_tag"])

        result = temp_sharded_cache.get_by_tag("shared_tag")
        assert len(result) == 3

        count = temp_sharded_cache.invalidate_tag("shared_tag")
        assert count == 3

    def test_sharded_stats_aggregate(self, temp_sharded_cache):
        """📊 Stats should aggregate from all shards."""
        for i in range(20):
            temp_sharded_cache.set(f"key_{i}", i)

        stats = temp_sharded_cache.stats()
        assert stats.key_count == 20

    def test_sharded_clear_all(self, temp_sharded_cache):
        """🧹 clear() should clear all shards."""
        for i in range(50):
            temp_sharded_cache.set(f"key_{i}", i)

        count = temp_sharded_cache.clear()
        assert count == 50

        stats = temp_sharded_cache.stats()
        assert stats.key_count == 0

    def test_sharded_get_many(self, temp_sharded_cache):
        """🔍 get_many() should work across shards."""
        items = {f"key_{i}": i for i in range(10)}
        temp_sharded_cache.set_many(items)

        result = temp_sharded_cache.get_many([f"key_{i}" for i in range(10)])
        assert len(result) == 10

    def test_sharded_invalid_shard_count(self, tmp_path):
        """⚠️ Shard count < 1 should raise ValueError."""
        with pytest.raises(ValueError, match="shards must be >= 1"):
            ShardedSQLiteBackend(tmp_path, shards=0)


# ═══════════════════════════════════════════════════════════════════════════════
# EDGE CASES
# ═══════════════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """🔬 Test edge cases and special scenarios."""

    def test_unicode_key(self, temp_cache):
        """🔤 Should handle unicode keys."""
        temp_cache.set("キー", "value")
        assert temp_cache.get("キー") == "value"

    def test_unicode_value(self, temp_cache):
        """🔤 Should handle unicode values."""
        temp_cache.set("key", "日本語テスト 🎉")
        assert temp_cache.get("key") == "日本語テスト 🎉"

    def test_empty_string_key(self, temp_cache):
        """📝 Should handle empty string key."""
        temp_cache.set("", "empty_key_value")
        assert temp_cache.get("") == "empty_key_value"

    def test_empty_string_value(self, temp_cache):
        """📝 Should handle empty string value."""
        temp_cache.set("key", "")
        assert temp_cache.get("key") == ""

    def test_large_value(self, temp_cache):
        """📚 Should handle large values."""
        large_data = {"data": "x" * 100000}  # 100KB
        temp_cache.set("large", large_data)
        result = temp_cache.get("large")
        assert result == large_data

    def test_special_characters_in_key(self, temp_cache):
        """🔣 Should handle special characters in key."""
        special_key = "key:with/special\\chars?and=symbols&more"
        temp_cache.set(special_key, "value")
        assert temp_cache.get(special_key) == "value"

    def test_newlines_in_value(self, temp_cache):
        """📝 Should handle newlines in value."""
        temp_cache.set("key", "line1\nline2\nline3")
        assert temp_cache.get("key") == "line1\nline2\nline3"

    def test_empty_tags_list(self, temp_cache):
        """🏷️ Empty tags list should work."""
        temp_cache.set("key", "value", tags=[])
        assert temp_cache.get("key") == "value"


# ═══════════════════════════════════════════════════════════════════════════════
# BACKEND REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════


class TestBackendRegistry:
    """🏭 Test backend registration and discovery."""

    def test_sqlite_backend_registered(self):
        """🏭 sqlite backend should be registered."""
        assert "sqlite" in list_backends()

    def test_sharded_backend_registered(self):
        """🏭 sharded backend should be registered."""
        assert "sharded" in list_backends()

    def test_get_backend_returns_class(self):
        """🔍 get_backend() should return backend class."""
        backend_cls = get_backend("sqlite")
        assert backend_cls is SQLiteCacheBackend

    def test_get_backend_unknown_raises(self):
        """⚠️ get_backend() should raise for unknown backend."""
        with pytest.raises(ValueError, match="Unknown backend"):
            get_backend("nonexistent_backend")

    def test_register_custom_backend(self):
        """🏭 Should be able to register custom backend."""

        class CustomBackend(CacheBackend):
            def get(self, key):
                return None

            def set(self, key, value, ttl=None, tags=None):
                return True

            def delete(self, key):
                return False

            def exists(self, key):
                return False

            def clear(self):
                return 0

            def get_many(self, keys):
                return {}

            def set_many(self, items, ttl=None, tags=None):
                return 0

            def delete_many(self, keys):
                return 0

            def invalidate_tag(self, tag):
                return 0

            def get_by_tag(self, tag):
                return {}

            def stats(self):
                return CacheStats()

            def cleanup_expired(self):
                return 0

            def evict_lru(self, count):
                return 0

            def get_or_set(self, key, factory, ttl=None, tags=None):
                return factory()

            async def get_or_set_async(self, key, factory, ttl=None, tags=None):
                return factory()

            def close(self):
                pass

        # Register the custom backend
        register_backend("custom_test", CustomBackend)

        try:
            assert "custom_test" in list_backends()
            assert get_backend("custom_test") is CustomBackend
        finally:
            # Cleanup: unregister the custom backend
            unregister_backend("custom_test")

    def test_register_duplicate_raises(self):
        """⚠️ register_backend() should raise for duplicate names."""
        with pytest.raises(ValueError, match="already registered"):
            # sqlite is already registered
            register_backend("sqlite", SQLiteCacheBackend)


# ═══════════════════════════════════════════════════════════════════════════════
# CACHE ENTRY DATACLASS
# ═══════════════════════════════════════════════════════════════════════════════


class TestCacheEntry:
    """📦 Test CacheEntry dataclass."""

    def test_cache_entry_creation(self):
        """📦 CacheEntry should be creatable with minimal args."""
        entry = CacheEntry(key="test", value="data")
        assert entry.key == "test"
        assert entry.value == "data"
        assert entry.tags == []

    def test_cache_entry_is_expired_false(self):
        """🔍 is_expired should return False when not expired."""
        from datetime import datetime, timedelta

        entry = CacheEntry(
            key="test", value="data", expires_at=datetime.now() + timedelta(hours=1)
        )
        assert entry.is_expired is False

    def test_cache_entry_is_expired_true(self):
        """🔍 is_expired should return True when expired."""
        from datetime import datetime, timedelta

        entry = CacheEntry(
            key="test", value="data", expires_at=datetime.now() - timedelta(hours=1)
        )
        assert entry.is_expired is True

    def test_cache_entry_no_expiry(self):
        """🔍 is_expired should return False when expires_at is None."""
        entry = CacheEntry(key="test", value="data", expires_at=None)
        assert entry.is_expired is False


# ═══════════════════════════════════════════════════════════════════════════════
# CACHE MANAGER SINGLETON
# ═══════════════════════════════════════════════════════════════════════════════


class TestCacheManagerSingleton:
    """🔒 Test Cache singleton behavior."""

    def test_singleton_same_instance(self):
        """🔒 Cache() should always return same instance."""
        cache1 = Cache()
        cache2 = Cache()
        assert cache1 is cache2

    def test_use_backend_switches_backend(self, tmp_path):
        """🔧 use_backend() should switch to new backend."""
        backend = SQLiteCacheBackend(tmp_path / "switched.db")
        cache.use_backend(backend)
        cache.set("test", "value")
        assert cache.get("test") == "value"

    def test_close_releases_backend(self, tmp_path):
        """🔌 close() should release backend."""
        backend = SQLiteCacheBackend(tmp_path / "close_test.db")
        cache.use_backend(backend)
        cache.close()
        assert cache._backend is None


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL ACCESS
# ═══════════════════════════════════════════════════════════════════════════════


class TestModuleLevelAccess:
    """📦 Test module-level function access."""

    def test_module_level_get_set(self, tmp_path):
        """📦 Module-level get/set should work."""
        from scriptman.cache import get
        from scriptman.cache import set as cache_set

        backend = SQLiteCacheBackend(tmp_path / "module_level.db")
        cache.use_backend(backend)

        cache_set("module_key", "module_value")
        assert get("module_key") == "module_value"

    def test_module_level_delete(self, tmp_path):
        """📦 Module-level delete should work."""
        from scriptman.cache import delete
        from scriptman.cache import set as cache_set

        backend = SQLiteCacheBackend(tmp_path / "module_delete.db")
        cache.use_backend(backend)

        cache_set("to_delete", "value")
        assert delete("to_delete") is True
