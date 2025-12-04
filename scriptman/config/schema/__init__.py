"""⚙️ Configuration schema definitions with sensible defaults.

This module provides the Pydantic models that define Scriptman's configuration
structure. Each nested config section is defined in its own module for clarity.

Usage:
    >>> from scriptman.config.schema import ConfigSchema
    >>> ConfigSchema.get_all_keys()
    ['data.dir', 'data.logs', 'logging.level', 'execution.concurrent']

    >>> ConfigSchema.get_default("logging.level")
    'INFO'
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo

# Import nested config models
from .cache import CacheConfig
from .data import DataConfig
from .execution import ExecutionConfig
from .logging import LoggingConfig
from .observe import ObserveConfig

__all__ = [
    "ConfigSchema",
    "CacheConfig",
    "DataConfig",
    "ExecutionConfig",
    "LoggingConfig",
    "ObserveConfig",
]


class ConfigSchema(BaseModel):
    """📋 Root Scriptman configuration schema.

    All fields have sensible defaults. Users only need to override
    what they want to change.

    Sections:
        data: Data storage configuration (base dir, subdirectories)
        logging: Logging configuration (level)
        execution: Script execution configuration (concurrency)
        observe: Observer/telemetry configuration
        cache: Cache module configuration (shards, size limits, TTL)

    Example:
        >>> schema = ConfigSchema()
        >>> schema.data.dir
        PosixPath('.data')
        >>> schema.logging.level
        'INFO'
        >>> schema.execution.concurrent
        True
        >>> schema.observe.enabled
        True
        >>> schema.cache.shards
        1
    """

    data: DataConfig = Field(default_factory=DataConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    observe: ObserveConfig = Field(default_factory=ObserveConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)

    @classmethod
    def get_field_info(cls, key_path: str) -> tuple[Any, str, Any]:
        """🔍 Get field type, description, and default for a dot-notation key.

        Args:
            key_path: Dot-notation path (e.g., "logging.level")

        Returns:
            Tuple of (type, description, default_value)

        Raises:
            KeyError: If field path doesn't exist

        Example:
            >>> ConfigSchema.get_field_info("logging.level")
            (typing.Literal['DEBUG', 'INFO', ...], 'Logging verbosity level', 'INFO')
        """
        keys = key_path.split(".")
        model: type[BaseModel] = cls  # cls is always a BaseModel subclass

        for i, key in enumerate(keys):
            # Check if field exists
            if key not in model.model_fields:
                raise KeyError(f"Unknown config key: {key_path}")

            field: FieldInfo = model.model_fields[key]

            # Last key - return its info
            if i == len(keys) - 1:
                default: Any = field.get_default()
                if isinstance(default, BaseModel):
                    default = default.model_dump()
                return (field.annotation, field.description or "", default)

            # Navigate into nested model
            annotation: type[Any] | None = field.annotation
            try:
                if annotation is None or not isinstance(annotation, type):
                    raise KeyError(f"Cannot traverse into non-model field: {key}")
                if not issubclass(annotation, BaseModel):
                    raise KeyError(f"Cannot traverse into non-model field: {key}")
                model = annotation
            except TypeError:
                # issubclass raises TypeError if annotation is not a class
                raise KeyError(f"Cannot traverse into non-model field: {key}") from None

        raise KeyError(f"Invalid key path: {key_path}")

    @classmethod
    def get_default(cls, key_path: str) -> Any:
        """🔍 Get default value for a config key.

        Args:
            key_path: Dot-notation path (e.g., "logging.level")

        Returns:
            Default value for the key

        Example:
            >>> ConfigSchema.get_default("logging.level")
            'INFO'
            >>> ConfigSchema.get_default("execution.concurrent")
            True
        """
        _, _, default = cls.get_field_info(key_path)
        return default

    @classmethod
    def get_all_keys(cls, prefix: str = "") -> list[str]:
        """🔍 Get all valid config keys as dot-notation strings (recursive).

        Handles deeply nested models like observe.store.path.

        Args:
            prefix: Optional prefix for recursive calls

        Returns:
            List of all config keys in dot notation

        Example:
            >>> ConfigSchema.get_all_keys()
            ['data.dir', 'data.logs', 'logging.level', 'observe.enabled', 'observe.store.path']
        """
        return _get_all_keys_from_model(cls, prefix)


def _get_all_keys_from_model(model: type[BaseModel], prefix: str = "") -> list[str]:
    """🔍 Recursively get all keys from a Pydantic model.

    This helper function handles arbitrary nesting depth.
    """
    keys: list[str] = []
    for name, field in model.model_fields.items():
        full_key = f"{prefix}.{name}" if prefix else name
        annotation = field.annotation

        try:
            if annotation is not None and isinstance(annotation, type):
                if issubclass(annotation, BaseModel):
                    # RECURSIVE call
                    nested_keys = _get_all_keys_from_model(annotation, full_key)
                    keys.extend(nested_keys)
                else:
                    keys.append(full_key)
            else:
                keys.append(full_key)
        except TypeError:
            # issubclass raises TypeError if annotation is not a class
            keys.append(full_key)
    return keys
