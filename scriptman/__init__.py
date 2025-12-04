# 🦸‍♂️ Scriptman - Python automation made simple.
#
# Usage:
#     >>> import scriptman
#     >>> scriptman.config.get("logging.level")
#     'INFO'
#     >>> scriptman.serialize(Path(".logs"))
#     '.logs'
#     >>> from scriptman import observe
#     >>> observe.info("Processing started")
#     >>> from scriptman import cache
#     >>> cache.set("key", "value", ttl=3600)

from __future__ import annotations

# Version
__version__ = "3.0.0"

# Re-export database module for scriptman.database.SQLiteClient() style access
# Re-export config module for scriptman.config.get() style access
from scriptman import config, database

# Cache module for caching
from scriptman.cache import cache

# Observer module for telemetry and observability
from scriptman.observe import observe

# Serialization utilities
from scriptman.serialization import serialize

# Type utilities for generic programming and sync/async handling
from scriptman.types import (
    AsyncFunc,
    AsyncLock,
    BaseModelT,
    Func,
    P,
    R,
    StampedeLock,
    T,
    is_async,
)

__all__ = [
    "__version__",
    "cache",
    "config",
    "database",
    "observe",
    "serialize",
    # Type utilities
    "T",
    "P",
    "R",
    "BaseModelT",
    "Func",
    "AsyncFunc",
    "is_async",
    "AsyncLock",
    "StampedeLock",
]
