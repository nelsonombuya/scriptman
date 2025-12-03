"""🔄 Simple serialization utilities.

Provides functions to convert Python objects to serializable formats
for config files, APIs, caching, and more.

Usage:
    >>> import scriptman
    >>> scriptman.serialize(Path(".logs"))
    '.logs'
    >>> scriptman.to_path(".logs")
    PosixPath('.logs')
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path, PurePath
from typing import Any
from uuid import UUID

__all__ = ["serialize", "to_path"]


def serialize(value: Any) -> Any:
    """🔄 Convert Python value to JSON/TOML-compatible format.

    Handles: Path, datetime, Enum, UUID, Decimal, Pydantic models,
    and nested dicts/lists.

    Args:
        value: Any Python value

    Returns:
        Serializable value (str, int, float, bool, list, dict, None)

    Example:
        >>> serialize(Path(".logs"))
        '.logs'
        >>> serialize({"path": Path(".logs"), "ts": datetime.now()})
        {'path': '.logs', 'ts': '2024-01-15T10:30:00'}
    """
    if value is None:
        return None

    # Path -> string
    if isinstance(value, PurePath):
        return str(value)

    # DateTime types -> ISO format
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()

    # Enum -> value
    if isinstance(value, Enum):
        return value.value

    # UUID -> string
    if isinstance(value, UUID):
        return str(value)

    # Decimal -> string (preserves precision)
    if isinstance(value, Decimal):
        return str(value)

    # Pydantic models
    if hasattr(value, "model_dump"):
        return serialize(value.model_dump())

    # Nested dict
    if isinstance(value, dict):
        return {k: serialize(v) for k, v in value.items()}

    # Nested list/tuple/set
    if isinstance(value, (list, tuple, set, frozenset)):
        return [serialize(v) for v in value]

    # Primitives pass through
    return value


def to_path(value: str | Path, *, resolve: bool = False) -> Path:
    """📁 Convert to Path, optionally resolving to absolute.

    Args:
        value: Path string or Path object
        resolve: If True, convert to absolute path

    Returns:
        Path object

    Example:
        >>> to_path(".logs")
        PosixPath('.logs')
        >>> to_path(".logs", resolve=True)
        PosixPath('/absolute/path/to/.logs')
    """
    path = Path(value)
    return path.resolve() if resolve else path
