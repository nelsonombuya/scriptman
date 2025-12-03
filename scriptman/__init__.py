# 🦸‍♂️ Scriptman - Python automation made simple.
#
# Usage:
#     >>> import scriptman
#     >>> scriptman.config.get("logging.level")
#     'INFO'
#     >>> scriptman.serialize(Path(".logs"))
#     '.logs'
#     >>> scriptman.to_path(".logs")
#     PosixPath('.logs')

from __future__ import annotations

# Version
__version__ = "3.0.0"

# Re-export database module for scriptman.database.SQLiteClient() style access
# Re-export config module for scriptman.config.get() style access
from scriptman import config, database

# Serialization utilities
from scriptman.serialization import serialize, to_path

__all__ = ["__version__", "config", "database", "serialize", "to_path"]
