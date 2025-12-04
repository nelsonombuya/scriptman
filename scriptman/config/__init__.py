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

    # Path resolution for data directories
    >>> config.resolve_path("logs")
    PosixPath('.data/logs')
    >>> config.ensure_path("db")  # Creates directory if needed
    PosixPath('.data/db')
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from typing import Any

from pydantic import TypeAdapter, ValidationError

from scriptman._internal import log

from .readers import (
    ConfigReader,
    EnvVarReader,
    auto_discover_reader,
    auto_discover_secrets_reader,
)
from .schema import ConfigSchema
from .schema.data import DataCategory, DataConfig

__all__ = [
    # Singleton and classes
    "config",
    "Config",
    "ConfigSchema",
    # Reading & writing
    "get",
    "set",
    "reset",
    "reset_all",
    # Overrides
    "override",
    "temporary",
    "clear_overrides",
    # Reader management
    "use_reader",
    "migrate_to",
    "generate_example",
    # Path resolution
    "resolve_path",
    "ensure_path",
    # New features
    "reload",
    "dump",
]


class Config:
    """⚙️ Scriptman configuration manager.

    Provides unified access to configuration with priority chain:
        1. Runtime overrides (config.override())
        2. Environment variables (SCRIPTMAN_*)
        3. Config file (scriptman.json default, or scriptman.toml/yaml if extras installed)
        4. Schema defaults

    Thread-safe singleton pattern ensures one instance across the application.

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

    # ─────────────────────────────────────────────────────────────
    # Thread-Safe Singleton
    # ─────────────────────────────────────────────────────────────

    _instance: Config | None = None
    _lock: Lock = Lock()
    __initialized: bool = False

    def __new__(cls, *args: Any, **kwargs: Any) -> Config:
        """🔒 Thread-safe singleton pattern."""
        if cls._instance is None:
            with cls._lock:
                # Double-check locking
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        reader: ConfigReader | None = None,
        *,
        _is_secrets: bool = False,
        _force_reinit: bool = False,
    ) -> None:
        """🚀 Initialize configuration manager.

        Args:
            reader: Custom reader, or None for auto-discovery.
            _is_secrets: Internal flag for secrets instance.
            _force_reinit: Force reinitialization (for testing only).
        """
        # Skip if already initialized (singleton)
        if self.__initialized and not _force_reinit:
            return

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
            # Create secrets instance without triggering singleton
            secrets_instance = object.__new__(Config)
            # Manually initialize secrets instance
            secrets_instance._schema = ConfigSchema
            secrets_instance._reader = auto_discover_secrets_reader()
            secrets_instance._store = secrets_instance._reader.read()
            secrets_instance._overrides = {}
            secrets_instance._env_reader = EnvVarReader()
            secrets_instance._is_secrets = True
            secrets_instance.__initialized = True
            self._secrets = secrets_instance

        self.__initialized = True

    # ─────────────────────────────────────────────────────────────
    # Debug Representation
    # ─────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        """🔍 Debug representation of config state."""
        return (
            f"Config("
            f"reader={self._reader.name!r}, "
            f"file={self._reader.file_path}, "
            f"keys={len(self.keys())}, "
            f"overrides={len(self._overrides)})"
        )

    def __str__(self) -> str:
        """📝 Human-readable config summary."""
        return f"Scriptman Config ({self._reader.name}: {self._reader.file_path})"

    @property
    def secrets(self) -> Config:
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
        # 1. Check overrides (highest priority) - stored as flat keys
        if key in self._overrides:
            return self._overrides[key]

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
        if key not in self:
            raise KeyError(f"Config key not found: {key}")
        return self.get(key)

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

    def dump(self) -> dict[str, Any]:
        """📤 Export entire configuration as a nested dictionary.

        Returns the merged view of all config sources respecting priority.
        Useful for debugging, serialization, and config inspection.

        Returns:
            Complete configuration as nested dict

        Example:
            >>> config.dump()
            {'logging': {'level': 'INFO'}, 'execution': {'concurrent': True}}
        """
        result: dict[str, Any] = {}
        for key in self.keys():
            value = self.get(key)
            self._set_nested(result, key, value)
        return result

    def reload(self) -> None:
        """🔄 Reload configuration from file.

        Re-reads the config file and updates the in-memory store.
        Useful for picking up external changes without restarting.
        Overrides are preserved.

        Example:
            >>> config.reload()
        """
        self._store = self._reader.read()
        log.debug(f"🔄 Reloaded config from {self._reader.name}")

        # Emit reload event
        self._emit_event(
            "Config reloaded",
            event_type="config.reloaded",
            reader=self._reader.name,
            file=str(self._reader.file_path),
        )

    # ─────────────────────────────────────────────────────────────
    # Path Resolution
    # ─────────────────────────────────────────────────────────────

    def resolve_path(self, category: DataCategory) -> Path:
        """📁 Resolve path for a data category.

        Returns the full path for a data category based on config.
        Uses data.dir as the base directory.

        Args:
            category: One of 'logs', 'db', 'cache', 'artifacts', 'observe'

        Returns:
            Full resolved path for the category

        Example:
            >>> config.resolve_path("logs")
            PosixPath('.data/logs')
            >>> config.resolve_path("db")
            PosixPath('.data/db')
        """
        # Build DataConfig from current config values
        data_config = DataConfig(
            dir=self.get("data.dir"),
            logs=self.get("data.logs"),
            db=self.get("data.db"),
            cache=self.get("data.cache"),
            artifacts=self.get("data.artifacts"),
        )
        return data_config.get_path(category)

    def ensure_path(self, category: DataCategory) -> Path:
        """📁 Resolve path for a data category, creating directory if needed.

        Same as resolve_path but also creates the directory.

        Args:
            category: One of 'logs', 'db', 'cache', 'artifacts', 'observe'

        Returns:
            Full resolved path (directory created if needed)

        Example:
            >>> config.ensure_path("logs")
            PosixPath('.data/logs')
            >>> Path('.data/logs').exists()
            True
        """
        # Build DataConfig from current config values
        data_config = DataConfig(
            dir=self.get("data.dir"),
            logs=self.get("data.logs"),
            db=self.get("data.db"),
            cache=self.get("data.cache"),
            artifacts=self.get("data.artifacts"),
        )
        return data_config.ensure_path(category)

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
        # Capture old value for event
        old_value = self.get(key)

        # Validate against schema
        self._validate(key, value)

        # Update in-memory store
        self._set_nested(self._store, key, value)

        # Persist to file
        if persist and self._reader.supports_write():
            self._reader.write(self._store)
            log.debug(f"✍️ Config updated: {key} = {value}")

        # Emit config change event
        self._emit_event(
            f"Config changed: {key}",
            event_type="config.changed",
            key=key,
            old_value=self._safe_serialize(old_value),
            new_value=self._safe_serialize(value),
            persisted=persist,
        )

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
        log.debug(f"🔄 Reset {key} to default: {default}")

    def reset_all(self) -> None:
        """🔄 Reset all config to schema defaults."""
        self._store = {}
        if self._reader.supports_write():
            self._reader.write(self._store)
        log.info("🔄 Reset all config to defaults")

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
                    old_value = self._overrides.get(flat_key)
                    self._validate(flat_key, flat_value)
                    self._overrides[flat_key] = flat_value

                    # Emit config change event
                    self._emit_event(
                        f"Config override: {flat_key}",
                        event_type="config.changed",
                        key=flat_key,
                        old_value=self._safe_serialize(old_value),
                        new_value=self._safe_serialize(flat_value),
                        is_override=True,
                    )
            else:
                old_value = self._overrides.get(key)
                self._validate(key, value)
                self._overrides[key] = value

                # Emit config change event
                self._emit_event(
                    f"Config override: {key}",
                    event_type="config.changed",
                    key=key,
                    old_value=self._safe_serialize(old_value),
                    new_value=self._safe_serialize(value),
                    is_override=True,
                )

    def clear_overrides(self) -> None:
        """🧹 Clear all runtime overrides."""
        self._overrides.clear()
        log.debug("🧹 Cleared all config overrides")

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

        # Emit start event
        self._emit_event(
            "Config temporary override started",
            event_type="config.override.start",
            overrides={k: self._safe_serialize(v) for k, v in kwargs.items()},
        )

        self.override(**kwargs)
        try:
            yield
        finally:
            self._overrides = previous
            # Emit end event
            self._emit_event(
                "Config temporary override ended",
                event_type="config.override.end",
                restored_keys=list(kwargs.keys()),
            )

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
            log.info(f"📁 Renamed {old_path} → {migrated_path}")

        # Switch to new reader
        self._reader = new_reader

        log.success(f"✅ Migrated config: {old_reader.name} → {new_reader.name}")

    def use_reader(self, reader: ConfigReader) -> None:
        """📖 Switch to a different config reader.

        Args:
            reader: New reader to use
        """
        self._reader = reader
        self._store = reader.read()
        log.debug(f"📖 Switched to {reader.name} reader")

    def generate_example(self, format: str = "json") -> str:
        """📝 Generate example configuration in specified format.

        Args:
            format: Output format ('json', 'toml', 'yaml', 'env', 'pyproject')

        Returns:
            Example configuration as a string

        Raises:
            ValueError: If format is unknown or dependencies not installed

        Example:
            >>> example = config.generate_example("json")
            >>> print(example)
        """
        reader: ConfigReader

        if format == "json":
            from .readers import JsonReader

            reader = JsonReader(Path.cwd() / "scriptman.json")
        elif format == "pyproject":
            try:
                from .readers.toml import TomlReader

                reader = TomlReader(Path.cwd() / "pyproject.toml", section="scriptman")
            except ImportError as e:
                raise ValueError(
                    f"⚠️ TOML support required for '{format}' format. "
                    f"Install with: pip install scriptman[toml]"
                ) from e
        elif format == "toml":
            try:
                from .readers.toml import TomlReader

                reader = TomlReader(Path.cwd() / "scriptman.toml")
            except ImportError as e:
                raise ValueError(
                    f"⚠️ TOML support required for '{format}' format. "
                    f"Install with: pip install scriptman[toml]"
                ) from e
        elif format in ("yaml", "yml"):
            try:
                from .readers.yaml import YamlReader

                reader = YamlReader(Path.cwd() / "scriptman.yaml")
            except ImportError as e:
                raise ValueError(
                    f"⚠️ YAML support required for '{format}' format. "
                    f"Install with: pip install scriptman[yaml]"
                ) from e
        elif format == "env":
            reader = EnvVarReader()
        else:
            raise ValueError(
                f"⚠️ Unknown format: {format}. Supported: json, toml, yaml, env, pyproject"
            )

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
            log.warning(f"⚠️ Unknown config key: {key}")
        except ValidationError as e:
            log.exception(f"⚠️ Invalid value for {key}: {e}")
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

    def _emit_event(
        self,
        message: str,
        event_type: str,
        **data: Any,
    ) -> None:
        """📝 Emit config change event (safe, won't fail if observe unavailable)."""
        try:
            from scriptman.observe import observe
            from scriptman.observe.event import EventType

            # Use the event type constants if available
            if event_type == "config.changed":
                event_type = EventType.CONFIG_CHANGED
            elif event_type == "config.override.start":
                event_type = EventType.CONFIG_OVERRIDE_START
            elif event_type == "config.override.end":
                event_type = EventType.CONFIG_OVERRIDE_END

            observe.event(message, event_type=event_type, **data)
        except Exception:
            # Don't fail config operations if observe isn't ready
            pass

    @staticmethod
    def _safe_serialize(value: Any) -> Any:
        """🔒 Safely serialize value for event data."""
        try:
            from scriptman.serialization import serialize

            return serialize(value)
        except Exception:
            return str(value)


# ─────────────────────────────────────────────────────────────────
# Module-level singleton
# ─────────────────────────────────────────────────────────────────

config = Config()

# Expose methods at module level for scriptman.config.get() style

# Reading & writing
get = config.get
set = config.set
reset = config.reset
reset_all = config.reset_all

# Overrides
override = config.override
temporary = config.temporary
clear_overrides = config.clear_overrides

# Reader management
use_reader = config.use_reader
migrate_to = config.migrate_to
generate_example = config.generate_example

# Path resolution helpers
resolve_path = config.resolve_path
ensure_path = config.ensure_path

# New features
reload = config.reload
dump = config.dump
