"""💾 Sharded SQLite cache backend — distributed for performance.

This module provides a sharded cache that distributes keys across multiple
SQLite databases for better write performance.

Usage:
    >>> from scriptman.cache.backends.sharded import ShardedSQLiteBackend
    >>> backend = ShardedSQLiteBackend(shards=4)
    >>> backend.set("key", "value", ttl=3600)
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from scriptman._internal import log
from scriptman.cache.backends import CacheBackend, CacheStats, register_backend
from scriptman.cache.backends.sqlite import SQLiteCacheBackend


class ShardedSQLiteBackend(CacheBackend):
    """💾 Sharded SQLite cache for better write performance.

    Distributes keys across multiple SQLite databases using consistent
    hashing. This reduces write contention and improves performance
    for high-concurrency workloads.

    Args:
        path: Base directory for shard databases
        shards: Number of shards (default: 4)
        max_size_bytes: Total max size across all shards (None = unlimited)
        eviction_batch_size: Entries to evict per shard per LRU pass

    Note:
        - Single-key ops route to the appropriate shard
        - Multi-key ops aggregate results from relevant shards
        - Tag ops fan out to ALL shards
        - Stats aggregate from all shards

    Example:
        >>> backend = ShardedSQLiteBackend(shards=4)
        >>> backend.set("user:123", {"name": "Alice"})
        True
        >>> backend.get("user:123")
        {'name': 'Alice'}
    """

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        shards: int = 4,
        max_size_bytes: int | None = None,
        eviction_batch_size: int = 100,
    ) -> None:
        """🚀 Initialize sharded cache backend.

        Args:
            path: Base directory for shard files (None = config default)
            shards: Number of shards (must be >= 1)
            max_size_bytes: Total max size (divided across shards)
            eviction_batch_size: Entries to evict per shard per LRU pass
        """
        if shards < 1:
            raise ValueError("shards must be >= 1")

        # Resolve path from config if not provided
        if path is None:
            from scriptman import config

            path = config.ensure_path("cache")
        else:
            path = Path(path)
            path.mkdir(parents=True, exist_ok=True)

        self._base_path = Path(path)
        self._shard_count = shards

        # Calculate per-shard size limit
        per_shard_limit = None
        if max_size_bytes is not None:
            per_shard_limit = max_size_bytes // shards

        # Create shard backends
        self._shards: list[SQLiteCacheBackend] = []
        for i in range(shards):
            shard_path = self._base_path / f"cache_shard_{i}.db"
            self._shards.append(
                SQLiteCacheBackend(
                    shard_path,
                    max_size_bytes=per_shard_limit,
                    eviction_batch_size=eviction_batch_size,
                )
            )

        log.debug(
            f"🗄️ Sharded cache initialized: {shards} shards",
            path=str(self._base_path),
            shards=shards,
        )

    def _get_shard(self, key: str) -> SQLiteCacheBackend:
        """🔑 Get the shard for a given key."""
        shard_index = hash(key) % self._shard_count
        return self._shards[shard_index]

    def _get_shard_index(self, key: str) -> int:
        """🔑 Get shard index for a given key."""
        return hash(key) % self._shard_count

    # ─────────────────────────────────────────────────────────────
    # Core Operations (delegate to appropriate shard)
    # ─────────────────────────────────────────────────────────────

    def get(self, key: str) -> Any | None:
        """🔍 Retrieve value by key."""
        return self._get_shard(key).get(key)

    def set(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> bool:
        """✍️ Store value with optional TTL and tags."""
        return self._get_shard(key).set(key, value, ttl=ttl, tags=tags)

    def delete(self, key: str) -> bool:
        """🧹 Delete value by key."""
        return self._get_shard(key).delete(key)

    def exists(self, key: str) -> bool:
        """🔍 Check if key exists and is not expired."""
        return self._get_shard(key).exists(key)

    def clear(self) -> int:
        """🧹 Delete all cached values from all shards."""
        total = 0
        for shard in self._shards:
            total += shard.clear()
        return total

    # ─────────────────────────────────────────────────────────────
    # Bulk Operations (aggregate across shards)
    # ─────────────────────────────────────────────────────────────

    def get_many(self, keys: list[str]) -> dict[str, Any]:
        """🔍 Retrieve multiple values by keys."""
        if not keys:
            return {}

        # Group keys by shard
        shard_keys: dict[int, list[str]] = {}
        for key in keys:
            shard_idx = self._get_shard_index(key)
            if shard_idx not in shard_keys:
                shard_keys[shard_idx] = []
            shard_keys[shard_idx].append(key)

        # Query each shard and aggregate
        result: dict[str, Any] = {}
        for shard_idx, shard_key_list in shard_keys.items():
            shard_result = self._shards[shard_idx].get_many(shard_key_list)
            result.update(shard_result)

        return result

    def set_many(
        self,
        items: dict[str, Any],
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> int:
        """✍️ Store multiple values."""
        if not items:
            return 0

        # Group items by shard
        shard_items: dict[int, dict[str, Any]] = {}
        for key, value in items.items():
            shard_idx = self._get_shard_index(key)
            if shard_idx not in shard_items:
                shard_items[shard_idx] = {}
            shard_items[shard_idx][key] = value

        # Set on each shard
        total = 0
        for shard_idx, shard_item_dict in shard_items.items():
            total += self._shards[shard_idx].set_many(shard_item_dict, ttl=ttl, tags=tags)

        return total

    def delete_many(self, keys: list[str]) -> int:
        """🧹 Delete multiple values."""
        if not keys:
            return 0

        # Group keys by shard
        shard_keys: dict[int, list[str]] = {}
        for key in keys:
            shard_idx = self._get_shard_index(key)
            if shard_idx not in shard_keys:
                shard_keys[shard_idx] = []
            shard_keys[shard_idx].append(key)

        # Delete from each shard
        total = 0
        for shard_idx, shard_key_list in shard_keys.items():
            total += self._shards[shard_idx].delete_many(shard_key_list)

        return total

    # ─────────────────────────────────────────────────────────────
    # Tag Operations (fan out to all shards)
    # ─────────────────────────────────────────────────────────────

    def invalidate_tag(self, tag: str) -> int:
        """🧹 Delete all values with the given tag (all shards)."""
        total = 0
        for shard in self._shards:
            total += shard.invalidate_tag(tag)
        return total

    def get_by_tag(self, tag: str) -> dict[str, Any]:
        """🔍 Retrieve all values with the given tag (all shards)."""
        result: dict[str, Any] = {}
        for shard in self._shards:
            result.update(shard.get_by_tag(tag))
        return result

    # ─────────────────────────────────────────────────────────────
    # Statistics (aggregate from all shards)
    # ─────────────────────────────────────────────────────────────

    def stats(self) -> CacheStats:
        """📊 Get aggregated cache statistics."""
        total_hits = 0
        total_misses = 0
        total_size = 0
        total_keys = 0

        for shard in self._shards:
            shard_stats = shard.stats()
            total_hits += shard_stats.hits
            total_misses += shard_stats.misses
            total_size += shard_stats.size_bytes
            total_keys += shard_stats.key_count

        return CacheStats(
            hits=total_hits,
            misses=total_misses,
            size_bytes=total_size,
            key_count=total_keys,
        )

    # ─────────────────────────────────────────────────────────────
    # Maintenance (fan out to all shards)
    # ─────────────────────────────────────────────────────────────

    def cleanup_expired(self) -> int:
        """🧹 Remove all expired entries from all shards."""
        total = 0
        for shard in self._shards:
            total += shard.cleanup_expired()
        return total

    def evict_lru(self, count: int) -> int:
        """🧹 Evict LRU entries from all shards.

        Distributes eviction count across shards proportionally.
        """
        per_shard = max(1, count // self._shard_count)
        total = 0
        for shard in self._shards:
            total += shard.evict_lru(per_shard)
        return total

    # ─────────────────────────────────────────────────────────────
    # Stampede Prevention
    # ─────────────────────────────────────────────────────────────

    def get_or_set(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> Any:
        """🔐 Get value or compute and cache it (sync)."""
        return self._get_shard(key).get_or_set(key, factory, ttl=ttl, tags=tags)

    async def get_or_set_async(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> Any:
        """🔐 Get value or compute and cache it (async)."""
        return await self._get_shard(key).get_or_set_async(
            key, factory, ttl=ttl, tags=tags
        )

    # ─────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        """🔌 Close all shard connections."""
        for shard in self._shards:
            shard.close()


# Register the backend
register_backend("sharded", ShardedSQLiteBackend)

__all__ = ["ShardedSQLiteBackend"]
