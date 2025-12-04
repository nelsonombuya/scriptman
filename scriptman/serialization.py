"""🔄 Simple serialization utilities.

Provides functions to convert Python objects to serializable formats
for config files, APIs, caching, and more.

Usage:
    >>> import scriptman
    >>> scriptman.serialize(Path(".logs"))
    '.logs'
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import PurePath
from typing import Any
from uuid import UUID

__all__ = ["serialize"]


def serialize(value: Any) -> Any:
    """🔄 Convert Python value to JSON/TOML-compatible format.

    Handles: Path, datetime, Enum, UUID, Decimal, Pydantic models,
    and nested dicts/lists.

    Args:
        value: Any Python value

    Returns:
        Serializable value (str, int, float, bool, list, dict, None)

    Raises:
        RecursionError: If value contains circular references.

    Note:
        Non-serializable custom types pass through unchanged.

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
    if isinstance(value, (datetime | date | time)):
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
    if isinstance(value, (list | tuple | set | frozenset)):
        return [serialize(v) for v in value]

    # Primitives pass through
    return value
