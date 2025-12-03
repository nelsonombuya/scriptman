"""⚙️ Scriptman configuration management.

Usage:
    >>> from scriptman import config
    >>> config.get("logging.level")
    'INFO'
    >>> config.get("execution.concurrent")
    True
    >>> config.set("logging.level", "DEBUG")

    # Runtime overrides (not persisted)
    >>> config.override(logging={"level": "DEBUG"})
    >>> with config.temporary(execution={"concurrent": False}):
    ...     # Runs with concurrent=False
    ...     pass
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from loguru import logger
from pydantic import TypeAdapter, ValidationError

from .readers import (
    ConfigReader,
    EnvVarReader,
    auto_discover_reader,
    auto_discover_secrets_reader,
)
from .schema import ConfigSchema

__all__ = ["config", "Config", "ConfigSchema"]


class Config:
    """⚙️ Scriptman configuration manager.

    Provides unified access to configuration with priority chain:
        1. Runtime overrides (config.override())
        2. Environment variables (SCRIPTMAN_*)
        3. Config file (scriptman.toml / pyproject.toml)
        4. Schema defaults

    Supports multiple config formats via pluggable readers.

    Attributes:
        secrets: Separate Config instance for sensitive values

    Example:
        >>> from scriptman import config
        >>> config.get("logging.level")
        'INFO'
        >>> config["execution.concurrent"]
        True
    """

    def __init__(
        self,
        reader: ConfigReader | None = None,
        *,
        _is_secrets: bool = False,
    ) -> None:
        """🚀 Initialize configuration manager.

        Args:
            reader: Custom reader, or None for auto-discovery.
            _is_secrets: Internal flag for secrets instance.
        """
        self._schema = ConfigSchema
        self._reader = reader or (
            auto_discover_secrets_reader() if _is_secrets else auto_discover_reader()
        )
        self._store: dict[str, Any] = self._reader.read()
        self._overrides: dict[str, Any] = {}
        self._env_reader = EnvVarReader()
        self._is_secrets = _is_secrets

        # Secrets sub-instance (only for main config)
        self._secrets: Config | None = None
        if not _is_secrets:
            self._secrets = Config(_is_secrets=True)

    @property
    def secrets(self) -> "Config":
        """🔐 Access secrets configuration."""
        if self._is_secrets:
            raise AttributeError("Cannot access secrets.secrets")

        if self._secrets is None:
            raise RuntimeError("Secrets instance not initialized")

        return self._secrets

    # ─────────────────────────────────────────────────────────────
    # Reading
    # ─────────────────────────────────────────────────────────────

    def get(self, key: str, default: Any = None) -> Any:
        """🔍 Get config value with priority chain.

        Priority: override > env var > file > schema default

        Args:
            key: Dot-notation key (e.g., "retry.max_delay")
            default: Fallback if key doesn't exist

        Returns:
            Configuration value

        Example:
            >>> config.get("logging.level")
            'INFO'
            >>> config.get("retry.max_delay", default=30)
            60.0
        """
        # 1. Check overrides (highest priority)
        if (value := self._get_nested(self._overrides, key)) is not None:
            return value

        # 2. Check environment variables
        env_data = self._env_reader.read()
        if (value := self._get_nested(env_data, key)) is not None:
            return value

        # 3. Check config file
        if (value := self._get_nested(self._store, key)) is not None:
            return value

        # 4. Return schema default or provided default
        try:
            return self._schema.get_default(key)
        except KeyError:
            return default

    def __getitem__(self, key: str) -> Any:
        """🔍 Bracket notation access: config["retry.max_delay"]"""
        value = self.get(key)
        if value is None:
            raise KeyError(f"Config key not found: {key}")
        return value

    def __contains__(self, key: str) -> bool:
        """🔍 Check if key exists: "logging.level" in config"""
        try:
            self._schema.get_field_info(key)
            return True
        except KeyError:
            return key in self._store

    def keys(self) -> list[str]:
        """🔍 Get all valid config keys."""
        return self._schema.get_all_keys()

    def items(self) -> list[tuple[str, Any]]:
        """🔍 Get all config key-value pairs."""
        return [(key, self.get(key)) for key in self.keys()]

    # ─────────────────────────────────────────────────────────────
    # Writing
    # ─────────────────────────────────────────────────────────────

    def set(self, key: str, value: Any, *, persist: bool = True) -> None:
        """✍️ Set config value, optionally persisting to file.

        Args:
            key: Dot-notation key (e.g., "retry.max_delay")
            value: Value to set (validated against schema)
            persist: Whether to write to config file

        Raises:
            ValidationError: If value doesn't match schema type
            ValueError: If key is unknown

        Example:
            >>> config.set("logging.level", "DEBUG")
            >>> config.set("execution.concurrent", False)
        """
        # Validate against schema
        self._validate(key, value)

        # Update in-memory store
        self._set_nested(self._store, key, value)

        # Persist to file
        if persist and self._reader.supports_write():
            self._reader.write(self._store)
            logger.debug(f"✍️ Config updated: {key} = {value}")

    def __setitem__(self, key: str, value: Any) -> None:
        """✍️ Bracket notation assignment: config["logging.level"] = "DEBUG" """
        self.set(key, value)

    def reset(self, key: str) -> None:
        """🔄 Reset a key to its schema default.

        Args:
            key: Dot-notation key to reset
        """
        default = self._schema.get_default(key)
        self.set(key, default)
        logger.debug(f"🔄 Reset {key} to default: {default}")

    def reset_all(self) -> None:
        """🔄 Reset all config to schema defaults."""
        self._store = {}
        if self._reader.supports_write():
            self._reader.write(self._store)
        logger.info("🔄 Reset all config to defaults")

    # ─────────────────────────────────────────────────────────────
    # Overrides
    # ─────────────────────────────────────────────────────────────

    def override(self, **kwargs: Any) -> None:
        """⚡ Set runtime overrides (not persisted to file).

        Overrides have highest priority in the config chain.
        Use clear_overrides() to remove.

        Args:
            **kwargs: Key-value pairs to override

        Example:
            >>> config.override(logging={"level": "DEBUG"})
            >>> config.override(execution={"concurrent": False})
        """
        for key, value in kwargs.items():
            if isinstance(value, dict):
                # Flatten nested dict
                for flat_key, flat_value in self._flatten_dict(value, prefix=key):
                    self._validate(flat_key, flat_value)
                    self._overrides[flat_key] = flat_value
            else:
                self._validate(key, value)
                self._overrides[key] = value

    def clear_overrides(self) -> None:
        """🧹 Clear all runtime overrides."""
        self._overrides.clear()
        logger.debug("🧹 Cleared all config overrides")

    @contextmanager
    def temporary(self, **kwargs: Any) -> Iterator[None]:
        """🔄 Context manager for temporary overrides.

        Overrides are automatically cleared when exiting the context.

        Args:
            **kwargs: Key-value pairs to override temporarily

        Example:
            >>> with config.temporary(
            ...     logging={"level": "DEBUG"},
            ...     execution={"concurrent": False},
            ... ):
            ...     # Runs with temporary config
            ...     pass
            >>> # Original config restored
        """
        previous = self._overrides.copy()
        self.override(**kwargs)
        try:
            yield
        finally:
            self._overrides = previous

    # ─────────────────────────────────────────────────────────────
    # Reader Management
    # ─────────────────────────────────────────────────────────────

    def migrate_to(self, new_reader: ConfigReader) -> None:
        """🔄 Migrate settings to a new reader/format.

        Writes current settings to the new reader and renames the old
        config file with .migrated suffix.

        Args:
            new_reader: Target reader to migrate to

        Raises:
            ValueError: If new reader doesn't support writing

        Example:
            >>> from scriptman.config.readers import TomlReader
            >>> config.migrate_to(TomlReader(Path("scriptman.yaml")))
            ✅ Migrated from scriptman.toml → scriptman.yaml
        """
        if not new_reader.supports_write():
            raise ValueError(
                f"⚠️ Cannot migrate to {new_reader.name}: reader doesn't support writing"
            )

        old_reader = self._reader
        old_path = old_reader.file_path

        # Write current store to new reader
        new_reader.write(self._store)

        # Rename old file
        if old_path and old_path.exists():
            migrated_path = old_path.with_suffix(old_path.suffix + ".migrated")
            old_path.rename(migrated_path)
            logger.info(f"📁 Renamed {old_path} → {migrated_path}")

        # Switch to new reader
        self._reader = new_reader

        logger.success(f"✅ Migrated config: {old_reader.name} → {new_reader.name}")

    def use_reader(self, reader: ConfigReader) -> None:
        """📖 Switch to a different config reader.

        Args:
            reader: New reader to use
        """
        self._reader = reader
        self._store = reader.read()
        logger.debug(f"📖 Switched to {reader.name} reader")

    def generate_example(self, format: str = "toml") -> str:
        """📝 Generate example configuration in specified format.

        Args:
            format: Output format ('toml', 'env', 'pyproject')

        Returns:
            Example configuration as a string

        Example:
            >>> example = config.generate_example("toml")
            >>> print(example)
        """
        from .readers import TomlReader

        reader: ConfigReader
        if format == "pyproject":
            reader = TomlReader(Path.cwd() / "pyproject.toml", section="scriptman")
        elif format == "toml":
            reader = TomlReader(Path.cwd() / "scriptman.toml")
        elif format == "env":
            reader = EnvVarReader()
        else:
            raise ValueError(f"⚠️ Unknown format: {format}")

        return reader.generate_example(self._schema)

    # ─────────────────────────────────────────────────────────────
    # Private Helpers
    # ─────────────────────────────────────────────────────────────

    def _validate(self, key: str, value: Any) -> None:
        """🔍 Validate a value against the schema."""
        try:
            field_type, _, _ = self._schema.get_field_info(key)
            adapter = TypeAdapter(field_type)
            adapter.validate_python(value)
        except KeyError:
            # Unknown key - allow for flexibility (user-defined keys)
            logger.warning(f"⚠️ Unknown config key: {key}")
        except ValidationError as e:
            raise ValueError(f"⚠️ Invalid value for {key}: {e}") from e

    @staticmethod
    def _get_nested(data: dict[str, Any], key: str) -> Any | None:
        """🔍 Get value from nested dict using dot notation."""
        keys = key.split(".")
        current = data

        for k in keys:
            if not isinstance(current, dict) or k not in current:
                return None
            current = current[k]

        return current

    @staticmethod
    def _set_nested(data: dict[str, Any], key: str, value: Any) -> None:
        """✍️ Set value in nested dict using dot notation."""
        keys = key.split(".")
        current = data

        for k in keys[:-1]:
            current = current.setdefault(k, {})

        current[keys[-1]] = value

    @staticmethod
    def _flatten_dict(
        data: dict[str, Any],
        prefix: str = "",
    ) -> list[tuple[str, Any]]:
        """🔄 Flatten nested dict to dot-notation key-value pairs."""
        items: list[tuple[str, Any]] = []

        for key, value in data.items():
            full_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                items.extend(Config._flatten_dict(value, full_key))
            else:
                items.append((full_key, value))

        return items


# ─────────────────────────────────────────────────────────────────
# Module-level singleton
# ─────────────────────────────────────────────────────────────────

config = Config()

# Expose methods at module level for scriptman.config.get() style
get = config.get
set = config.set
reset = config.reset
reset_all = config.reset_all

override = config.override
temporary = config.temporary
clear_overrides = config.clear_overrides

use_reader = config.use_reader
migrate_to = config.migrate_to
generate_example = config.generate_example
