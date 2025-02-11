try:
    from abc import ABC
    from enum import Enum
    from os import remove
    from pathlib import Path
    from typing import Any, Optional

    from diskcache import FanoutCache
    from loguru import logger

    from scriptman.core.config import config
    from scriptman.powers.cache._backend import CacheBackend
except ImportError:
    raise ImportError(
        "DiskCache backend is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[cache]"
    )

"""DiskCache Backends"""


class EvictionPolicy(Enum):
    LRS = "least-recently-stored"
    LRU = "least-recently-used"
    LFU = "least-frequently-used"
    NONE = "none"


class DiskCacheBackend(CacheBackend, ABC):
    """Abstract Base Class for DiskCache implementations of the cache backend."""

    _CACHE_DIR: Path = Path(config.get("disk_cache_dir", "../../../cache"))

    def get(self, key: str, retry: bool = True, **kwargs: Any) -> Any:
        return self.cache.get(key=key, retry=retry, **kwargs)

    def set(self, key: str, value: Any, ttl: Optional[int] = None, **kwargs: Any) -> bool:
        return bool(self.cache.set(key=key, value=value, expire=ttl, **kwargs))

    def delete(self, key: str, **kwargs: Any) -> bool:
        return bool(self.cache.delete(key, **kwargs))

    @staticmethod
    def clean_cache_dir() -> None:
        from datetime import datetime

        now = datetime.now()

        # Remove all *.db files not modified in the past 24 hours
        for file in DiskCacheBackend._CACHE_DIR.glob("*.db"):
            file_mod_time = datetime.fromtimestamp(file.stat().st_mtime)
            if (now - file_mod_time).total_seconds() > 24 * 60 * 60:
                try:
                    remove(file)
                    logger.debug(f"🚮 Removed file: {file}")
                except Exception as e:
                    logger.error(f"💥 Error removing file {file}: {e}")

        # Remove all empty folders
        for folder in DiskCacheBackend._CACHE_DIR.glob("*"):
            if folder.is_dir() and len(list(folder.iterdir())) == 0:
                try:
                    folder.rmdir()
                    logger.debug(f"🚮 Removed empty folder: {folder}")
                except Exception as e:
                    logger.error(f"💥 Error removing empty folder {folder}: {e}")


class FanoutCacheBackend(DiskCacheBackend):
    """DiskCache implementation of the cache backend using FanoutCache"""

    def __init__(
        self,
        directory: Optional[Path] = None,
        shards: int = 8,
        statistics: bool = True,
        eviction_policy: EvictionPolicy = EvictionPolicy.LRS,
        **kwargs: Any,
    ):
        self._cache = FanoutCache(
            __directory=directory or str(FanoutCacheBackend._CACHE_DIR),
            eviction_policy=eviction_policy.value,
            statistics=statistics,
            shards=shards,
            **kwargs,
        )

    @property
    def cache(self) -> FanoutCache:
        return self._cache
