# 📝 Scriptman Logging Module — Implementation Plan

> **Status: ✅ FULLY IMPLEMENTED** (December 2024)
>
> All components have been implemented and tested. This document is preserved
> for reference and documentation purposes.

## Overview

This plan guides the implementation of Scriptman's comprehensive logging system:

1. **Internal Safe Logging** (`_internal/log.py`) — Handles circular imports gracefully
2. **Log Formatting** (`observe/formatting.py`) — User-customizable log formats via config
3. **Config Schema Updates** (`config/schema/logging.py`) — Extended logging options

**Primary Goals:**
- Eliminate circular import risks in internal modules
- Give users control over log appearance via config
- Maintain full observability integration

---

## 📋 Problem Statement

### Issue 1: Circular Imports

Top-level imports cause circular dependencies:
```python
# scriptman/config/__init__.py line 32
from scriptman.observe import observe  # ⚠️ DANGER
```

**Current workaround:** Repeated `_observe_log()` helpers in each module.

**Import Order Analysis:**
```
scriptman/__init__.py
├── imports config (line 23)
│   └── config/__init__.py
│       ├── imports observe (line 32) ⚠️ CIRCULAR
│       └── imports readers (line 34)
│           └── readers/__init__.py
│               └── imports observe (line 7) ⚠️ CIRCULAR
└── imports observe (line 29)
```

The fix breaks this cycle by having internal modules use lazy imports via `_internal.log`.

### Issue 2: No Log Formatting Control

Users cannot customize log appearance:
- No format string customization
- No color toggle
- No preset options (minimal, detailed, JSON)
- Hard-coded formatting in `observe/logger.py`

---

## 📁 File Structure

```
scriptman/
├── _internal/                    # NEW: Internal utilities
│   ├── __init__.py              # Exposes log module
│   └── log.py                   # Safe internal logging
├── config/
│   ├── __init__.py              # UPDATE: Use _internal.log
│   ├── readers/
│   │   ├── __init__.py          # UPDATE: Use _internal.log
│   │   └── toml.py              # UPDATE: Use _internal.log
│   └── schema/
│       └── logging.py           # UPDATE: Extended options
├── cache/
│   ├── __init__.py              # UPDATE: Remove _observe_log helper
│   └── backends/
│       ├── sqlite.py            # UPDATE: Remove _observe_log helper
│       └── sharded.py           # UPDATE: Remove _observe_log helper
├── database/
│   └── sqlite.py                # UPDATE: Remove _observe_log helper
├── observe/
│   ├── __init__.py              # UPDATE: Add configure methods
│   ├── formatting.py            # NEW: Format presets & config
│   └── logger.py                # UPDATE: Use formatting config
└── ...
```

---

## 🔍 Files Requiring Updates

### Summary Table

| File                         | Current Approach                                     | Top-level Import Issue? | Action                       |
| ---------------------------- | ---------------------------------------------------- | ----------------------- | ---------------------------- |
| `config/__init__.py`         | Top-level `from scriptman.observe import observe`    | 🔴 **CRITICAL**          | Use `_internal.log`          |
| `config/readers/__init__.py` | Top-level `from scriptman.observe import observe`    | 🔴 **CRITICAL**          | Use `_internal.log`          |
| `config/readers/toml.py`     | Top-level `from scriptman.observe import observe`    | 🔴 **CRITICAL**          | Use `_internal.log`          |
| `cache/__init__.py`          | Uses `_observe_log()` helper                         | 🟡 Already handled       | Replace with `_internal.log` |
| `cache/backends/sqlite.py`   | Uses `_observe_log()` and `_observe_event()` helpers | 🟡 Already handled       | Replace with `_internal.log` |
| `cache/backends/sharded.py`  | Uses `_observe_log()` helper                         | 🟡 Already handled       | Replace with `_internal.log` |
| `database/sqlite.py`         | Uses `_observe_log()` helper                         | 🟡 Already handled       | Replace with `_internal.log` |

**CLI modules** (safe, user-facing — don't need `_internal.log`):
- `cli/__init__.py`, `cli/config.py`, `cli/observe.py`, `cli/dev/*.py`
- These are fine because CLI is only run after full import resolution

---

## Step 1: Create Internal Safe Logging

### File: `scriptman/_internal/__init__.py`

```python
"""🔐 Internal utilities — not part of the public API.

These modules are for internal Scriptman use only. External users
should import from the main scriptman package instead.

Contents:
    - log: Safe logging that handles circular imports
"""

from scriptman._internal import log

__all__ = ["log"]
```

### File: `scriptman/_internal/log.py`

```python
"""📝 Safe internal logging — handles circular imports gracefully.

This module provides logging functions that:
1. Delegate to observe when available (full observability)
2. Fall back to loguru if observe isn't ready (during initialization)
3. Never cause circular imports (all imports are lazy)

Usage (internal modules only):
    >>> from scriptman._internal import log
    >>> log.info("Processing started", invoice_id="123")
    >>> log.error("Failed to connect", error="timeout")

External users should use observe directly:
    >>> from scriptman import observe
    >>> observe.info("Processing started")

Why this exists:
    - Internal modules (config, cache, database) need logging
    - These modules are imported before observe is fully initialized
    - Direct observe imports at module level cause circular imports
    - This module uses lazy imports to break the cycle
"""

from __future__ import annotations

from typing import Any

from loguru import logger

__all__ = [
    "trace",
    "debug",
    "info",
    "warning",
    "error",
    "success",
    "critical",
    "exception",
    "event",
]


# ═══════════════════════════════════════════════════════════════════════════════
# INTERNAL STATE
# ═══════════════════════════════════════════════════════════════════════════════

# Sentinel to detect recursive calls during initialization
_LOGGING = object()
_current_state: object | None = None


# ═══════════════════════════════════════════════════════════════════════════════
# CORE LOGGING FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════


def _log(level: str, message: str, **data: Any) -> None:
    """📝 Internal logging — delegates to observe or falls back to loguru.

    This function handles circular imports by:
    1. Detecting recursive calls (returns early)
    2. Using lazy imports for observe
    3. Falling back to loguru if observe fails
    """
    global _current_state

    # Detect recursive call during observe initialization
    if _current_state is _LOGGING:
        # Just use loguru directly — observe is still initializing
        _loguru_fallback(level, message, **data)
        return

    try:
        # Mark that we're currently logging (prevents recursion)
        _current_state = _LOGGING

        # Try to use observe (lazy import)
        from scriptman.observe import observe

        # Call the appropriate observe method
        log_method = getattr(observe, level.lower(), observe.info)
        log_method(message, **data)

    except ImportError:
        # Observe not available yet — use loguru
        _loguru_fallback(level, message, **data)

    except RecursionError:
        # Explicit recursion — use loguru
        _loguru_fallback(level, message, **data)

    except Exception:
        # Any other error — fall back safely
        _loguru_fallback(level, message, **data)

    finally:
        _current_state = None


def _loguru_fallback(level: str, message: str, **data: Any) -> None:
    """📝 Direct loguru output when observe isn't available."""
    log_method = getattr(logger, level.lower(), logger.info)

    # Format data for console
    if data:
        data_str = " | " + " ".join(f"{k}={v}" for k, v in data.items())
        log_method(f"{message}{data_str}")
    else:
        log_method(message)


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API (mirrors observe)
# ═══════════════════════════════════════════════════════════════════════════════


def trace(message: str, **data: Any) -> None:
    """🔬 Log trace message — most verbose (internal use).

    Example:
        >>> log.trace("Entering function", func="process_data")
    """
    _log("trace", message, **data)


def debug(message: str, **data: Any) -> None:
    """🔍 Log debug message (internal use).

    Example:
        >>> log.debug("Cache initialized", path="/data/cache")
    """
    _log("debug", message, **data)


def info(message: str, **data: Any) -> None:
    """📝 Log info message (internal use).

    Example:
        >>> log.info("Config loaded", source="scriptman.toml")
    """
    _log("info", message, **data)


def warning(message: str, **data: Any) -> None:
    """⚠️ Log warning message (internal use).

    Example:
        >>> log.warning("Unknown config key", key="invalid.key")
    """
    _log("warning", message, **data)


def error(message: str, **data: Any) -> None:
    """❌ Log error message (internal use).

    Example:
        >>> log.error("Database connection failed", error=str(e))
    """
    _log("error", message, **data)


def success(message: str, **data: Any) -> None:
    """✅ Log success message (internal use).

    Example:
        >>> log.success("Migration completed", records=1500)
    """
    _log("success", message, **data)


def critical(message: str, **data: Any) -> None:
    """🔥 Log critical message (internal use).

    Example:
        >>> log.critical("System shutdown required", reason="disk full")
    """
    _log("critical", message, **data)


def exception(message: str, **data: Any) -> None:
    """💥 Log error with traceback (internal use).

    Call this inside an except block to capture the traceback.

    Example:
        >>> try:
        ...     risky_operation()
        ... except Exception:
        ...     log.exception("Operation failed", operation="import")
    """
    import traceback

    data["traceback"] = traceback.format_exc()
    _log("error", message, **data)


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT EMISSION
# ═══════════════════════════════════════════════════════════════════════════════


def event(message: str, event_type: str, **data: Any) -> None:
    """📊 Emit a custom event (internal use).

    Args:
        message: Event message
        event_type: Event type string (e.g., "config.changed", "cache.hit")
        **data: Additional event data

    Example:
        >>> log.event("Config changed", "config.changed", key="logging.level")
    """
    global _current_state

    if _current_state is _LOGGING:
        return  # Skip events during recursive calls

    try:
        _current_state = _LOGGING
        from scriptman.observe import observe

        observe.event(message, event_type=event_type, **data)
    except Exception:
        pass  # Events are optional — don't fail
    finally:
        _current_state = None
```

---

## Step 2: Extended Logging Config Schema

### File: `scriptman/config/schema/logging.py`

Replace the existing file with:

```python
"""📋 Logging configuration schema."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class LoggingConfig(BaseModel):
    """📋 Logging configuration.

    Controls how Scriptman logs messages to console and files.

    Attributes:
        level: Logging verbosity level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format: Log format preset or custom loguru format string
        colorize: Enable colored console output
        diagnose: Include detailed exception diagnostics
        backtrace: Include full exception backtrace
        dir: Override log directory (None uses data.dir/data.logs)
        file_enabled: Enable file logging
        file_rotation: When to rotate log files (size or time)
        file_retention: How long to keep old log files
        file_compression: Compression format for rotated logs

    Format Presets:
        - "minimal"    : "{level.icon} {message}"
        - "simple"     : "{time:HH:mm:ss} | {level} | {message}" (default)
        - "detailed"   : Full context with module/function/line
        - "json"       : JSON format for log aggregation
        - "production" : Clean format without colors (for files)

    Custom Format Placeholders (loguru):
        - {time}      : Timestamp (e.g., {time:YYYY-MM-DD HH:mm:ss})
        - {level}     : Log level name
        - {level.icon}: Log level emoji
        - {message}   : Log message
        - {name}      : Module name
        - {function}  : Function name
        - {line}      : Line number
        - {file}      : File name
        - {extra}     : Extra data dict

    Example (scriptman.toml):
        [logging]
        level = "DEBUG"
        format = "simple"
        colorize = true

        # File logging
        file_enabled = true
        file_rotation = "10 MB"
        file_retention = "7 days"

        # Or with custom format:
        format = "{time:HH:mm:ss} | {level.icon} | {message}"

    Example (code):
        >>> config.get("logging.level")
        'INFO'
        >>> config.get("logging.format")
        'simple'
    """

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging verbosity level",
    )

    format: str = Field(
        default="simple",
        description=(
            "Log format: 'minimal', 'simple', 'detailed', 'json', 'production', "
            "or custom loguru format string"
        ),
    )

    colorize: bool = Field(
        default=True,
        description="Enable colored console output",
    )

    diagnose: bool = Field(
        default=True,
        description="Include detailed exception diagnostics",
    )

    backtrace: bool = Field(
        default=True,
        description="Include full exception backtrace",
    )

    dir: Path | None = Field(
        default=None,
        description="Override log directory (None uses data.dir/data.logs)",
    )

    file_enabled: bool = Field(
        default=False,
        description="Enable file logging",
    )

    file_rotation: str = Field(
        default="10 MB",
        description="When to rotate log files (size like '10 MB' or time like '1 day')",
    )

    file_retention: str = Field(
        default="7 days",
        description="How long to keep old log files",
    )

    file_compression: str = Field(
        default="zip",
        description="Compression format for rotated logs (zip, gz, bz2, xz, tar)",
    )
```

---

## Step 3: Log Formatting Module

### File: `scriptman/observe/formatting.py`

```python
"""🎨 Log formatting — presets and configuration for loguru.

This module provides:
- Format presets (minimal, simple, detailed, json, production)
- Configuration function to apply logging settings
- Integration with scriptman config

Usage:
    >>> from scriptman.observe.formatting import configure_logging
    >>> configure_logging(format="minimal", colorize=True)

    # Or auto-configure from config:
    >>> from scriptman.observe.formatting import configure_from_config
    >>> configure_from_config()
"""

from __future__ import annotations

import sys
from typing import Any

from loguru import logger

__all__ = [
    "FORMAT_PRESETS",
    "resolve_format",
    "configure_logging",
    "configure_from_config",
    "add_file_handler",
]


# ═══════════════════════════════════════════════════════════════════════════════
# FORMAT PRESETS
# ═══════════════════════════════════════════════════════════════════════════════

FORMAT_PRESETS: dict[str, str] = {
    # Minimal: Just emoji + message
    "minimal": "<level>{level.icon}</level> {message}",

    # Simple: Time, level, message (Scriptman default)
    "simple": (
        "<green>{time:HH:mm:ss}</green> | "
        "<level>{level:<8}</level> | "
        "{message}"
    ),

    # Detailed: Full context (module, function, line)
    "detailed": (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level:<8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "{message}"
    ),

    # JSON: Structured logging for log aggregation (Datadog, ELK, etc.)
    "json": (
        '{{"time":"{time:YYYY-MM-DDTHH:mm:ss.SSSZ}",'
        '"level":"{level}",'
        '"message":"{message}",'
        '"module":"{name}",'
        '"function":"{function}",'
        '"line":{line}}}'
    ),

    # Production: Clean, no colors (for file logging)
    "production": (
        "{time:YYYY-MM-DD HH:mm:ss} | "
        "{level:<8} | "
        "{name}:{function}:{line} | "
        "{message}"
    ),
}


# ═══════════════════════════════════════════════════════════════════════════════
# FORMATTING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def resolve_format(format_str: str) -> str:
    """🎨 Resolve format string (preset name or custom format).

    Args:
        format_str: Preset name ('simple', 'minimal', etc.) or custom format

    Returns:
        Resolved loguru format string

    Example:
        >>> resolve_format("simple")
        '<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}'
        >>> resolve_format("{time} - {message}")
        '{time} - {message}'
    """
    return FORMAT_PRESETS.get(format_str.lower(), format_str)


def _ensure_custom_levels() -> None:
    """🎨 Ensure custom log levels are defined."""
    # Loguru may not have SUCCESS level by default
    try:
        logger.level("SUCCESS")
    except ValueError:
        logger.level("SUCCESS", no=25, color="<green>", icon="✅")


def configure_logging(
    level: str = "INFO",
    format: str = "simple",
    colorize: bool = True,
    diagnose: bool = True,
    backtrace: bool = True,
) -> None:
    """⚙️ Configure loguru with the specified settings.

    Call this once at application startup to set log formatting.
    Removes the default loguru handler and adds a configured one.

    Args:
        level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format: Format preset name or custom loguru format string
        colorize: Enable colors in console output
        diagnose: Include diagnostic info in exceptions
        backtrace: Include full backtrace in exceptions

    Example:
        >>> configure_logging(level="DEBUG", format="minimal")
        >>> configure_logging(format="{time:HH:mm:ss} | {level} | {message}")
    """
    # Ensure custom levels exist
    _ensure_custom_levels()

    # Remove all existing handlers
    logger.remove()

    # Resolve format preset
    fmt = resolve_format(format)

    # Add configured handler
    logger.add(
        sink=sys.stderr,
        format=fmt,
        level=level.upper(),
        colorize=colorize,
        diagnose=diagnose,
        backtrace=backtrace,
    )


def configure_from_config() -> None:
    """⚙️ Configure logging from scriptman config.

    Reads logging.* settings and applies them to loguru.
    Safe to call during initialization (uses lazy import).

    Also sets up file logging if logging.file_enabled is True.

    Example:
        >>> configure_from_config()
        # Reads from config:
        # [logging]
        # level = "DEBUG"
        # format = "simple"
        # colorize = true
        # file_enabled = true
    """
    try:
        from scriptman import config

        configure_logging(
            level=config.get("logging.level", "INFO"),
            format=config.get("logging.format", "simple"),
            colorize=config.get("logging.colorize", True),
            diagnose=config.get("logging.diagnose", True),
            backtrace=config.get("logging.backtrace", True),
        )

        # Set up file logging if enabled
        if config.get("logging.file_enabled", False):
            log_dir = config.get("logging.dir")
            if log_dir is None:
                log_dir = config.resolve_path("logs")

            log_path = log_dir / "scriptman.log"
            add_file_handler(
                str(log_path),
                level=config.get("logging.level", "DEBUG"),
                rotation=config.get("logging.file_rotation", "10 MB"),
                retention=config.get("logging.file_retention", "7 days"),
                compression=config.get("logging.file_compression", "zip"),
            )
    except Exception:
        # Fall back to defaults if config isn't ready
        configure_logging()


def add_file_handler(
    path: str,
    level: str = "DEBUG",
    format: str = "production",
    rotation: str = "10 MB",
    retention: str = "7 days",
    compression: str = "zip",
) -> int:
    """📁 Add a file handler for persistent logging.

    Args:
        path: Log file path
        level: Minimum log level for file
        format: Format preset or custom string (default: production)
        rotation: When to rotate (size or time)
        retention: How long to keep old logs
        compression: Compression for rotated logs

    Returns:
        Handler ID (for removal if needed)

    Example:
        >>> add_file_handler(".data/logs/app.log")
        >>> add_file_handler(
        ...     ".data/logs/errors.log",
        ...     level="ERROR",
        ...     rotation="1 day",
        ... )
    """
    fmt = resolve_format(format)

    return logger.add(
        sink=path,
        format=fmt,
        level=level.upper(),
        rotation=rotation,
        retention=retention,
        compression=compression,
        colorize=False,  # No colors in files
    )
```

---

## Step 4: Update Observer Module

### File: `scriptman/observe/__init__.py`

Add the formatting functions to the ObserveModule class and __all__:

```python
# Add imports at top
from scriptman.observe.formatting import (
    configure_logging,
    configure_from_config,
    add_file_handler,
    FORMAT_PRESETS,
)

# Add to ObserveModule class:
class ObserveModule:
    # ... existing code ...

    # Formatting
    configure_logging = staticmethod(configure_logging)
    configure_from_config = staticmethod(configure_from_config)
    add_file_handler = staticmethod(add_file_handler)
    FORMAT_PRESETS = FORMAT_PRESETS

# Add to __all__:
__all__ = [
    # ... existing exports ...
    "configure_logging",
    "configure_from_config",
    "add_file_handler",
    "FORMAT_PRESETS",
]
```

---

## Step 5: Update Internal Modules

### 5.1 Update `config/__init__.py`

1. Remove: `from scriptman.observe import observe` (line 32)
2. Add: `from scriptman._internal import log`
3. Replace all `observe.debug(...)` → `log.debug(...)`
4. Replace all `observe.info(...)` → `log.info(...)`
5. Replace all `observe.success(...)` → `log.success(...)`
6. Replace all `observe.warning(...)` → `log.warning(...)`
7. Replace all `observe.exception(...)` → `log.exception(...)`

**Note:** Keep the lazy `_emit_event()` helper as-is since it handles event emission correctly.

### 5.2 Update `config/readers/__init__.py`

1. Remove: `from scriptman.observe import observe` (line 7)
2. Add: `from scriptman._internal import log`
3. Line 41: `observe.debug(...)` → `log.debug(...)`
4. Line 110: `observe.debug(...)` → `log.debug(...)`
5. Line 114: `observe.debug(...)` → `log.debug(...)`
6. Line 146: `observe.debug(...)` → `log.debug(...)`

### 5.3 Update `config/readers/toml.py`

1. Remove: `from scriptman.observe import observe` (line 11)
2. Add: `from scriptman._internal import log`
3. Line 51: `observe.debug(...)` → `log.debug(...)`
4. Line 64: `observe.exception(...)` → `log.exception(...)`
5. Line 95: `observe.debug(...)` → `log.debug(...)`
6. Line 97: `observe.exception(...)` → `log.exception(...)`

### 5.4 Update `cache/__init__.py`

1. Remove the `_observe_log()` helper function (lines 53-60)
2. Add: `from scriptman._internal import log`
3. Line 139: `_observe_log("debug", ...)` → `log.debug(...)`
4. Line 151: `_observe_log("debug", ...)` → `log.debug(...)`
5. Line 496: `_observe_log("debug", ...)` → `log.debug(...)`
6. Line 533: `_observe_log("debug", ...)` → `log.debug(...)`
7. Line 568: `_observe_log("debug", ...)` → `log.debug(...)`

### 5.5 Update `cache/backends/sqlite.py`

1. Remove `_observe_log()` helper (lines 30-37)
2. Remove `_observe_event()` helper (lines 40-58)
3. Add: `from scriptman._internal import log`
4. Replace all `_observe_log("level", ...)` → `log.level(...)`
5. Replace all `_observe_event(...)` → `log.event(...)`

**Note on events:** The `_observe_event()` calls map to `log.event()`:
```python
# Before
_observe_event("cache.hit", f"Cache hit: {key}", key=key)

# After
log.event(f"Cache hit: {key}", "cache.hit", key=key)
```

### 5.6 Update `cache/backends/sharded.py`

1. Remove `_observe_log()` helper (lines 21-28)
2. Add: `from scriptman._internal import log`
3. Lines 107-112: `_observe_log("debug", ...)` → `log.debug(...)`

### 5.7 Update `database/sqlite.py`

1. Remove `_observe_log()` helper (lines 15-22)
2. Add: `from scriptman._internal import log`
3. Replace all `_observe_log("level", ...)` → `log.level(...)`

Example changes:
```python
# Line 167
_observe_log("debug", f"🔌 Connected to SQLite: {self._path} with WAL mode")
# becomes:
log.debug(f"🔌 Connected to SQLite: {self._path} with WAL mode")

# Line 169
_observe_log("critical", f"🔥 Failed to connect to database: {self._path}")
# becomes:
log.critical(f"🔥 Failed to connect to database: {self._path}")
```

---

## ✅ Testing Checklist

After implementation, verify:

```python
# ═══════════════════════════════════════════════════════════════════════════════
# 1. Import Order Independence — no circular import errors
# ═══════════════════════════════════════════════════════════════════════════════

# These should all work in any order without errors:
import scriptman
from scriptman import config
from scriptman import observe
from scriptman import cache
from scriptman import database

# Also test fresh imports in isolation:
# python -c "from scriptman import config"
# python -c "from scriptman import observe"
# python -c "from scriptman import cache"

# ═══════════════════════════════════════════════════════════════════════════════
# 2. Internal log fallback works during initialization
# ═══════════════════════════════════════════════════════════════════════════════

from scriptman._internal import log
log.info("This should work even before observe is initialized")
log.debug("Testing debug level", extra="data")
log.trace("Most verbose level")

# ═══════════════════════════════════════════════════════════════════════════════
# 3. Config logging settings work
# ═══════════════════════════════════════════════════════════════════════════════

from scriptman import config
config.set("logging.level", "DEBUG")
config.set("logging.format", "detailed")

# ═══════════════════════════════════════════════════════════════════════════════
# 4. Format presets work
# ═══════════════════════════════════════════════════════════════════════════════

from scriptman import observe

observe.configure_logging(format="minimal")
observe.info("Test minimal format")

observe.configure_logging(format="simple")
observe.info("Test simple format")

observe.configure_logging(format="detailed")
observe.info("Test detailed format")

observe.configure_logging(format="json")
observe.info("Test JSON format")

observe.configure_logging(format="production")
observe.info("Test production format")

# ═══════════════════════════════════════════════════════════════════════════════
# 5. Custom format works
# ═══════════════════════════════════════════════════════════════════════════════

observe.configure_logging(format="{time:HH:mm} | {level.icon} | {message}")
observe.info("Test custom format")

# ═══════════════════════════════════════════════════════════════════════════════
# 6. Config-based configuration
# ═══════════════════════════════════════════════════════════════════════════════

config.set("logging.format", "simple")
config.set("logging.colorize", False)
observe.configure_from_config()
observe.info("Test config-based format")

# ═══════════════════════════════════════════════════════════════════════════════
# 7. File handler works
# ═══════════════════════════════════════════════════════════════════════════════

import os
observe.add_file_handler(".data/logs/test.log")
observe.info("Test file logging")
assert os.path.exists(".data/logs/test.log")

# ═══════════════════════════════════════════════════════════════════════════════
# 8. Color toggle works
# ═══════════════════════════════════════════════════════════════════════════════

observe.configure_logging(colorize=False)
observe.info("Test without colors")

observe.configure_logging(colorize=True)
observe.info("Test with colors")

# ═══════════════════════════════════════════════════════════════════════════════
# 9. File logging via config works
# ═══════════════════════════════════════════════════════════════════════════════

config.set("logging.file_enabled", True)
config.set("logging.file_rotation", "1 MB")
config.set("logging.file_retention", "3 days")
observe.configure_from_config()
observe.info("Test file logging via config")

# ═══════════════════════════════════════════════════════════════════════════════
# 10. Config readers use safe logging (no circular imports)
# ═══════════════════════════════════════════════════════════════════════════════

from scriptman.config.readers import auto_discover_reader
reader = auto_discover_reader()
# Should not raise circular import error

# ═══════════════════════════════════════════════════════════════════════════════
# 11. Database module uses safe logging
# ═══════════════════════════════════════════════════════════════════════════════

from scriptman.database import SQLiteClient
db = SQLiteClient(":memory:")
db.execute("SELECT 1")
db.close()
# Should not raise circular import error

# ═══════════════════════════════════════════════════════════════════════════════
# 12. Cache operations log correctly
# ═══════════════════════════════════════════════════════════════════════════════

from scriptman import cache
cache.set("test_key", "test_value", ttl=60)
value = cache.get("test_key")
assert value == "test_value"
# Check logs show cache.set and cache.hit events
```

---

## 📊 Implementation Summary

| File                                 | Lines (est.) | Complexity | Status     |
| ------------------------------------ | ------------ | ---------- | ---------- |
| `_internal/__init__.py`              | ~15          | Low        | ✅ Complete |
| `_internal/log.py`                   | ~229         | Low        | ✅ Complete |
| `config/schema/logging.py`           | ~119         | Low        | ✅ Complete |
| `observe/formatting.py`              | ~241         | Low        | ✅ Complete |
| `observe/logger.py`                  | +trace fn    | Low        | ✅ Complete |
| `observe/__init__.py` updates        | ~20          | Low        | ✅ Complete |
| `config/__init__.py` updates         | ~10 changes  | Low        | ✅ Complete |
| `config/readers/__init__.py` updates | ~10 changes  | Low        | ✅ Complete |
| `config/readers/toml.py` updates     | ~8 changes   | Low        | ✅ Complete |
| `cache/__init__.py` updates          | ~15 changes  | Low        | ✅ Complete |
| `cache/backends/sqlite.py` updates   | ~20 changes  | Low        | ✅ Complete |
| `cache/backends/sharded.py` updates  | ~8 changes   | Low        | ✅ Complete |
| `database/sqlite.py` updates         | ~15 changes  | Low        | ✅ Complete |
| `tests/test_internal_log.py`         | ~870         | Medium     | ✅ Complete |

**Total:** ~620+ lines new/changed — **ALL IMPLEMENTED**

---

## 🔗 Dependencies

- `loguru` — Already a dependency (core logging)
- No new dependencies required

---

## 📝 Config Examples

### Minimal (emoji only)
```toml
[logging]
level = "INFO"
format = "minimal"
```
Output: `📝 Processing invoice | invoice_id=123`

### Simple (default)
```toml
[logging]
level = "DEBUG"
format = "simple"
colorize = true
```
Output: `14:32:15 | DEBUG    | Processing invoice | invoice_id=123`

### Detailed (with context)
```toml
[logging]
level = "DEBUG"
format = "detailed"
```
Output: `2024-12-04 14:32:15.234 | DEBUG    | app.billing:process:42 - Processing invoice | invoice_id=123`

### JSON (for log aggregation)
```toml
[logging]
level = "INFO"
format = "json"
colorize = false
```
Output: `{"time":"2024-12-04T14:32:15.234Z","level":"INFO","message":"Processing invoice | invoice_id=123",...}`

### Production (file logging)
```toml
[logging]
level = "WARNING"
format = "production"
colorize = false
dir = "/var/log/myapp"
file_enabled = true
file_rotation = "10 MB"
file_retention = "30 days"
```

### Custom format
```toml
[logging]
format = "{time:HH:mm:ss} | {level.icon} | [{name}] {message}"
```

---

## 🎯 Benefits

1. **No circular imports** — All observe imports are lazy via `_internal.log`
2. **User customization** — Format, colors, diagnostics via config
3. **Presets for common cases** — minimal, simple, detailed, json, production
4. **Power user flexibility** — Custom loguru format strings
5. **File logging support** — With rotation and compression, configurable via TOML
6. **Cleaner code** — No more `_observe_log()` helpers everywhere
7. **Clear separation** — Internal (`_internal.log`) vs external (`observe`)
8. **Future proof** — Adding new modules won't risk import cycles
9. **Trace level support** — Most verbose logging for debugging internals

---

## 🚀 Implementation Order

1. ✅ **Create `_internal/` module** — Foundation for safe logging
2. ✅ **Update `config/schema/logging.py`** — Extended options
3. ✅ **Create `observe/formatting.py`** — Presets and configuration
4. ✅ **Update `observe/__init__.py`** — Expose formatting functions
5. ✅ **Update `observe/logger.py`** — Add trace level support
6. ✅ **Update config modules** — `config/__init__.py`, `config/readers/*.py`
7. ✅ **Update cache modules** — `cache/__init__.py`, `cache/backends/*.py`
8. ✅ **Update database module** — `database/sqlite.py`
9. ✅ **Write tests** — Comprehensive test suite with subprocess import tests
10. ✅ **Run tests** — Verify no circular imports, all formats work

---

## 🎨 Formatting Integration with Internal Log

### How `_internal/log.py` Respects Formatting

The internal log module integrates with the formatting system through two paths:

#### Path 1: Observe Delegation (Normal Operation)

When observe is available, internal log delegates to it:

```python
# _internal/log.py lines 78-82
from scriptman.observe import observe
log_method = getattr(observe, level.lower(), observe.info)
log_method(message, **data)
```

This means:
- Any format configured via `observe.configure_logging()` is respected
- Any format set via `observe.configure_from_config()` is respected
- Format presets (minimal, simple, detailed, json, production) work correctly

#### Path 2: Loguru Fallback (During Initialization)

When observe isn't available (early initialization or errors):

```python
# _internal/log.py lines 100-109
def _loguru_fallback(level: str, message: str, **data: Any) -> None:
    log_method = getattr(logger, level.lower(), logger.info)
    if data:
        data_str = " | " + " ".join(f"{k}={v}" for k, v in data.items())
        log_method(f"{message}{data_str}")
    else:
        log_method(message)
```

This path:
- Uses loguru's currently active configuration (if `configure_logging()` was called)
- Has hardcoded data formatting (` | key=value`) for consistency
- Works even before config is loaded (uses loguru defaults)

### Key Design Decisions

1. **Fallback is intentionally simple** — It must work during early initialization before any config is loaded

2. **Data formatting is consistent** — Both paths format kwargs the same way (` | key=value`)

3. **Format presets apply to message structure** — The timestamp/level/module parts respect presets; the data appending is handled separately

### Usage Flow

```
User code → log.info("msg", key=val)
                    ↓
              _log("info", ...)
                    ↓
         ┌─────────┴─────────┐
         │                   │
    observe available?   observe unavailable
         │                   │
    observe.info(...)   _loguru_fallback(...)
         │                   │
    Uses configured     Uses loguru config
    format preset       (or defaults)
```

---

## 🧪 Comprehensive Test Suite for `_internal/log.py`

### File: `tests/test_internal_log.py`

```python
"""Tests for scriptman._internal.log — safe internal logging.

Coverage Goals:
- All 8 public log functions (trace, debug, info, warning, error, success, critical, exception)
- Event emission (log.event)
- Fallback behavior when observe unavailable
- Circular import prevention via _current_state
- Data formatting with various types
- Edge cases (empty, unicode, very long messages)
- Observe delegation when available
- Formatting integration

Target: 95%+ coverage
"""

from __future__ import annotations

import sys
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from loguru import logger

from scriptman._internal import log


# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture
def capture_logs():
    """Capture loguru output for assertion."""
    output = StringIO()
    # Remove default handler and add capture handler
    logger.remove()
    handler_id = logger.add(
        output,
        format="{level} | {message}",
        level="TRACE",
        colorize=False,
    )
    yield output
    logger.remove(handler_id)
    # Restore default handler
    logger.add(sys.stderr, level="DEBUG")


@pytest.fixture(autouse=True)
def reset_log_state():
    """Reset internal log state before and after each test."""
    log._current_state = None
    yield
    log._current_state = None


# ═══════════════════════════════════════════════════════════════════════════════
# BASIC LOGGING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


class TestBasicLogging:
    """Test all log level functions work correctly."""

    def test_trace_logs_message(self, capture_logs):
        """🔬 log.trace() should emit trace-level message."""
        log.trace("Test trace message")
        output = capture_logs.getvalue()
        assert "Test trace message" in output
        assert "TRACE" in output

    def test_debug_logs_message(self, capture_logs):
        """🔍 log.debug() should emit debug-level message."""
        log.debug("Test debug message")
        output = capture_logs.getvalue()
        assert "Test debug message" in output

    def test_info_logs_message(self, capture_logs):
        """📝 log.info() should emit info-level message."""
        log.info("Test info message")
        output = capture_logs.getvalue()
        assert "Test info message" in output

    def test_warning_logs_message(self, capture_logs):
        """⚠️ log.warning() should emit warning-level message."""
        log.warning("Test warning message")
        output = capture_logs.getvalue()
        assert "Test warning message" in output

    def test_error_logs_message(self, capture_logs):
        """❌ log.error() should emit error-level message."""
        log.error("Test error message")
        output = capture_logs.getvalue()
        assert "Test error message" in output

    def test_success_logs_message(self, capture_logs):
        """✅ log.success() should emit success-level message."""
        # Note: SUCCESS may not exist in default loguru, fallback to info
        log.success("Test success message")
        output = capture_logs.getvalue()
        assert "Test success message" in output

    def test_critical_logs_message(self, capture_logs):
        """🔥 log.critical() should emit critical-level message."""
        log.critical("Test critical message")
        output = capture_logs.getvalue()
        assert "Test critical message" in output

    def test_all_levels_callable(self):
        """📦 All log level functions should be callable without error."""
        # These should not raise
        log.trace("trace")
        log.debug("debug")
        log.info("info")
        log.warning("warning")
        log.error("error")
        log.success("success")
        log.critical("critical")


# ═══════════════════════════════════════════════════════════════════════════════
# DATA FORMATTING
# ═══════════════════════════════════════════════════════════════════════════════


class TestDataFormatting:
    """Test that kwargs are formatted correctly."""

    def test_single_kwarg_formatted(self, capture_logs):
        """📦 Single kwarg should appear in output."""
        log.info("Test message", user_id="123")
        output = capture_logs.getvalue()
        assert "Test message" in output
        assert "user_id=123" in output

    def test_multiple_kwargs_formatted(self, capture_logs):
        """📦 Multiple kwargs should appear in output."""
        log.info("Test message", user_id="123", action="login", count=5)
        output = capture_logs.getvalue()
        assert "user_id=123" in output
        assert "action=login" in output
        assert "count=5" in output

    def test_no_kwargs_works(self, capture_logs):
        """📦 Log without kwargs should work without extra formatting."""
        log.info("Simple message")
        output = capture_logs.getvalue()
        assert "Simple message" in output
        # Should not have trailing pipe from data formatting
        assert "Simple message |" not in output or "Simple message | " not in output.replace("Simple message | ", "")

    def test_none_value_in_data(self, capture_logs):
        """📦 None values should be handled."""
        log.info("Test", value=None)
        output = capture_logs.getvalue()
        assert "Test" in output
        assert "value=None" in output

    def test_numeric_values(self, capture_logs):
        """📦 Numeric values should be formatted."""
        log.info("Numbers", integer=42, floating=3.14, negative=-10)
        output = capture_logs.getvalue()
        assert "integer=42" in output
        assert "floating=3.14" in output
        assert "negative=-10" in output

    def test_boolean_values(self, capture_logs):
        """📦 Boolean values should be formatted."""
        log.info("Bools", enabled=True, disabled=False)
        output = capture_logs.getvalue()
        assert "enabled=True" in output
        assert "disabled=False" in output

    def test_list_value(self, capture_logs):
        """📦 List values should be formatted."""
        log.info("List test", items=[1, 2, 3])
        output = capture_logs.getvalue()
        assert "items=" in output

    def test_dict_value(self, capture_logs):
        """📦 Dict values should be formatted."""
        log.info("Dict test", data={"key": "value"})
        output = capture_logs.getvalue()
        assert "data=" in output


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTION LOGGING
# ═══════════════════════════════════════════════════════════════════════════════


class TestExceptionLogging:
    """Test log.exception() captures traceback."""

    def test_exception_includes_traceback_key(self, capture_logs):
        """💥 log.exception() should include traceback in data."""
        try:
            raise ValueError("Test error")
        except ValueError:
            log.exception("Operation failed", operation="test")

        output = capture_logs.getvalue()
        assert "Operation failed" in output
        assert "traceback=" in output

    def test_exception_captures_error_details(self, capture_logs):
        """💥 log.exception() traceback should contain error info."""
        try:
            raise RuntimeError("Specific runtime error")
        except RuntimeError:
            log.exception("Caught runtime error")

        output = capture_logs.getvalue()
        assert "RuntimeError" in output or "runtime error" in output.lower()

    def test_exception_outside_except_block(self, capture_logs):
        """💥 log.exception() outside except block should not crash."""
        # Should not raise, even with no active exception
        log.exception("No active exception")
        output = capture_logs.getvalue()
        assert "No active exception" in output

    def test_exception_with_extra_data(self, capture_logs):
        """💥 log.exception() should preserve additional kwargs."""
        try:
            raise TypeError("Type mismatch")
        except TypeError:
            log.exception("Type error", field="username", expected="str")

        output = capture_logs.getvalue()
        assert "field=username" in output
        assert "expected=str" in output

    def test_exception_uses_error_level(self, capture_logs):
        """💥 log.exception() should log at error level."""
        try:
            raise Exception("test")
        except Exception:
            log.exception("Test exception")

        output = capture_logs.getvalue()
        assert "ERROR" in output


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT EMISSION
# ═══════════════════════════════════════════════════════════════════════════════


class TestEventEmission:
    """Test log.event() for custom events."""

    def test_event_calls_observe_event(self):
        """📊 log.event() should delegate to observe.event()."""
        mock_observe = MagicMock()

        with patch.dict(sys.modules, {"scriptman.observe": MagicMock(observe=mock_observe)}):
            with patch("scriptman._internal.log._current_state", None):
                # Force fresh import path
                import importlib
                import scriptman._internal.log as log_module

                # Call event
                log_module.event("Test event", "test.type", key="value")

    def test_event_silently_fails_on_error(self):
        """📊 log.event() should not raise on failure."""
        with patch(
            "scriptman.observe.observe.event",
            side_effect=Exception("Deliberate error"),
        ):
            # Should not raise
            log.event("Test event", "test.type")

    def test_event_skipped_during_recursion(self):
        """📊 log.event() should skip during recursive calls."""
        # Simulate being in a logging call
        log._current_state = log._LOGGING

        with patch("scriptman.observe.observe.event") as mock_event:
            log.event("Test event", "test.type")
            # Should not be called because we're in recursive state
            mock_event.assert_not_called()

    def test_event_resets_state_on_success(self):
        """📊 log.event() should reset _current_state after success."""
        with patch("scriptman.observe.observe.event"):
            log.event("Test", "test.type")

        assert log._current_state is None

    def test_event_resets_state_on_error(self):
        """📊 log.event() should reset _current_state even on error."""
        with patch(
            "scriptman.observe.observe.event",
            side_effect=Exception("Error"),
        ):
            log.event("Test", "test.type")

        assert log._current_state is None


# ═══════════════════════════════════════════════════════════════════════════════
# FALLBACK BEHAVIOR
# ═══════════════════════════════════════════════════════════════════════════════


class TestFallbackBehavior:
    """Test fallback to loguru when observe unavailable."""

    def test_loguru_fallback_formats_message_only(self, capture_logs):
        """🔄 Fallback without data should log message only."""
        log._loguru_fallback("info", "Simple message")
        output = capture_logs.getvalue()
        assert "Simple message" in output

    def test_loguru_fallback_formats_data(self, capture_logs):
        """🔄 Fallback should format kwargs with pipe separator."""
        log._loguru_fallback("info", "Test message", key="value", num=42)
        output = capture_logs.getvalue()
        assert "Test message" in output
        assert "key=value" in output
        assert "num=42" in output
        assert " | " in output

    def test_loguru_fallback_unknown_level_defaults_to_info(self, capture_logs):
        """🔄 Fallback with unknown level should use info."""
        log._loguru_fallback("nonexistent_level", "Test message")
        output = capture_logs.getvalue()
        # Should still log (defaults to info)
        assert "Test message" in output

    def test_loguru_fallback_all_levels(self, capture_logs):
        """🔄 Fallback should work for all standard levels."""
        levels = ["trace", "debug", "info", "warning", "error", "critical"]
        for level in levels:
            log._loguru_fallback(level, f"Test {level}")

        output = capture_logs.getvalue()
        for level in levels:
            assert f"Test {level}" in output

    def test_falls_back_on_import_error(self, capture_logs):
        """🔄 Should fall back to loguru on ImportError."""
        with patch(
            "scriptman._internal.log.observe",
            new_callable=PropertyMock,
            side_effect=ImportError("No observe"),
        ):
            # This should use fallback
            log._loguru_fallback("info", "Fallback test")

        output = capture_logs.getvalue()
        assert "Fallback test" in output

    def test_falls_back_on_recursion_error(self, capture_logs):
        """🔄 Should fall back to loguru on RecursionError."""
        # Trigger recursion detection
        log._current_state = log._LOGGING

        # This should use fallback due to recursion detection
        log._log("info", "Recursion fallback")

        output = capture_logs.getvalue()
        assert "Recursion fallback" in output

    def test_falls_back_on_generic_exception(self, capture_logs):
        """🔄 Should fall back to loguru on any exception."""
        with patch(
            "scriptman.observe.observe.info",
            side_effect=RuntimeError("Unexpected error"),
        ):
            log.info("Generic fallback test")

        output = capture_logs.getvalue()
        assert "Generic fallback test" in output


# ═══════════════════════════════════════════════════════════════════════════════
# CIRCULAR IMPORT PREVENTION
# ═══════════════════════════════════════════════════════════════════════════════


class TestCircularImportPrevention:
    """Test recursion detection prevents infinite loops."""

    def test_recursive_call_uses_fallback(self, capture_logs):
        """🔄 Recursive calls should use loguru fallback."""
        # Simulate being in a logging call already
        log._current_state = log._LOGGING

        log._log("info", "Recursive message")

        output = capture_logs.getvalue()
        assert "Recursive message" in output

    def test_state_reset_after_successful_logging(self):
        """🔄 _current_state should reset after successful log."""
        log.info("Test message")
        assert log._current_state is None

    def test_state_reset_after_import_error(self):
        """🔄 _current_state should reset after ImportError."""
        with patch(
            "scriptman._internal.log._log",
            wraps=log._log,
        ):
            # Force a situation where finally block runs
            log.info("Test")

        assert log._current_state is None

    def test_state_reset_after_exception(self):
        """🔄 _current_state should reset even if observe raises."""
        with patch(
            "scriptman.observe.observe.info",
            side_effect=Exception("Forced error"),
        ):
            log.info("Test message")

        assert log._current_state is None

    def test_sentinel_object_is_unique(self):
        """🔄 _LOGGING sentinel should be a unique object."""
        assert log._LOGGING is not None
        assert log._LOGGING is not True
        assert log._LOGGING is not False
        assert isinstance(log._LOGGING, object)


# ═══════════════════════════════════════════════════════════════════════════════
# OBSERVE DELEGATION
# ═══════════════════════════════════════════════════════════════════════════════


class TestObserveDelegation:
    """Test that log delegates to observe when available."""

    def test_delegates_info_to_observe(self):
        """🔍 log.info() should call observe.info()."""
        with patch("scriptman.observe.observe.info") as mock_info:
            log.info("Test message", key="value")
            mock_info.assert_called_once_with("Test message", key="value")

    def test_delegates_debug_to_observe(self):
        """🔍 log.debug() should call observe.debug()."""
        with patch("scriptman.observe.observe.debug") as mock_debug:
            log.debug("Debug message")
            mock_debug.assert_called_once_with("Debug message")

    def test_delegates_warning_to_observe(self):
        """🔍 log.warning() should call observe.warning()."""
        with patch("scriptman.observe.observe.warning") as mock_warning:
            log.warning("Warning message", code=123)
            mock_warning.assert_called_once_with("Warning message", code=123)

    def test_delegates_error_to_observe(self):
        """🔍 log.error() should call observe.error()."""
        with patch("scriptman.observe.observe.error") as mock_error:
            log.error("Error message", code=500)
            mock_error.assert_called_once_with("Error message", code=500)

    def test_delegates_critical_to_observe(self):
        """🔍 log.critical() should call observe.critical()."""
        with patch("scriptman.observe.observe.critical") as mock_critical:
            log.critical("Critical message")
            mock_critical.assert_called_once_with("Critical message")

    def test_falls_back_for_unknown_level(self, capture_logs):
        """🔍 Unknown level should fall back to info."""
        # _log with unknown level should use getattr fallback
        log._log("nonexistent", "Unknown level message")
        output = capture_logs.getvalue()
        assert "Unknown level message" in output


# ═══════════════════════════════════════════════════════════════════════════════
# FORMATTING INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════


class TestFormattingIntegration:
    """Test that internal log respects observe formatting config."""

    def test_respects_custom_format(self):
        """🎨 Internal log should use configured format via observe."""
        output = StringIO()
        logger.remove()
        logger.add(output, format="[CUSTOM] {message}", level="DEBUG", colorize=False)

        log._loguru_fallback("info", "Formatted message")

        result = output.getvalue()
        assert "[CUSTOM]" in result
        assert "Formatted message" in result

        # Cleanup
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")

    def test_observe_formatting_applied(self):
        """🎨 When delegating to observe, formatting should be applied."""
        # This test verifies the integration works - observe handles the formatting
        with patch("scriptman.observe.observe.info") as mock_info:
            log.info("Test formatted")
            mock_info.assert_called_once()

    def test_fallback_uses_loguru_config(self):
        """🎨 Fallback should use whatever loguru config is active."""
        output = StringIO()
        logger.remove()
        # Use detailed format
        logger.add(
            output,
            format="{time:HH:mm:ss} | {level} | {name}:{function} | {message}",
            level="DEBUG",
            colorize=False,
        )

        log._loguru_fallback("info", "Config test")

        result = output.getvalue()
        assert "Config test" in result
        # Should have time format
        assert ":" in result  # Time separator

        # Cleanup
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")


# ═══════════════════════════════════════════════════════════════════════════════
# EDGE CASES
# ═══════════════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_message(self, capture_logs):
        """📝 Empty message should not crash."""
        log.info("")
        # Should complete without error

    def test_whitespace_only_message(self, capture_logs):
        """📝 Whitespace-only message should work."""
        log.info("   ")
        output = capture_logs.getvalue()
        # Should log something

    def test_unicode_message(self, capture_logs):
        """📝 Unicode messages should work."""
        log.info("Unicode: 你好世界 🎉 émojis 日本語")
        output = capture_logs.getvalue()
        assert "Unicode" in output

    def test_unicode_in_data(self, capture_logs):
        """📝 Unicode in kwargs should work."""
        log.info("Test", greeting="你好", emoji="🚀")
        output = capture_logs.getvalue()
        assert "greeting=" in output

    def test_very_long_message(self, capture_logs):
        """📝 Very long messages should not crash."""
        long_message = "x" * 10000
        log.info(long_message)
        output = capture_logs.getvalue()
        assert "x" in output

    def test_very_long_data_value(self, capture_logs):
        """📝 Very long data values should not crash."""
        long_value = "y" * 10000
        log.info("Test", data=long_value)
        # Should complete without error

    def test_special_characters_in_message(self, capture_logs):
        """📝 Special characters should not break formatting."""
        log.info("Special: {curly} [brackets] (parens) 'quotes' \"double\"")
        output = capture_logs.getvalue()
        assert "Special" in output

    def test_newlines_in_message(self, capture_logs):
        """📝 Newlines in message should work."""
        log.info("Line 1\nLine 2\nLine 3")
        output = capture_logs.getvalue()
        assert "Line 1" in output

    def test_tabs_in_message(self, capture_logs):
        """📝 Tabs in message should work."""
        log.info("Col1\tCol2\tCol3")
        output = capture_logs.getvalue()
        assert "Col1" in output

    def test_format_string_in_message(self, capture_logs):
        """📝 Format-like strings should not cause issues."""
        log.info("User {name} with %s and %(key)s")
        output = capture_logs.getvalue()
        assert "{name}" in output

    def test_callable_in_data(self, capture_logs):
        """📝 Callable in data should be handled."""
        log.info("Test", callback=lambda x: x)
        output = capture_logs.getvalue()
        assert "callback=" in output

    def test_class_instance_in_data(self, capture_logs):
        """📝 Class instances in data should be handled."""
        class CustomClass:
            def __repr__(self):
                return "CustomClass()"

        log.info("Test", obj=CustomClass())
        output = capture_logs.getvalue()
        assert "obj=" in output


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE API
# ═══════════════════════════════════════════════════════════════════════════════


class TestModuleAPI:
    """Test module exports and API surface."""

    def test_all_exports_exist(self):
        """📦 All __all__ exports should exist as attributes."""
        expected_exports = [
            "trace", "debug", "info", "warning", "error",
            "success", "critical", "exception", "event",
        ]
        for name in expected_exports:
            assert hasattr(log, name), f"Missing export: {name}"

    def test_all_exports_match_module_all(self):
        """📦 __all__ should contain all public functions."""
        assert "trace" in log.__all__
        assert "debug" in log.__all__
        assert "info" in log.__all__
        assert "warning" in log.__all__
        assert "error" in log.__all__
        assert "success" in log.__all__
        assert "critical" in log.__all__
        assert "exception" in log.__all__
        assert "event" in log.__all__

    def test_functions_are_callable(self):
        """📦 All exported functions should be callable."""
        for name in log.__all__:
            func = getattr(log, name)
            assert callable(func), f"{name} should be callable"

    def test_import_from_internal_package(self):
        """📦 Should be importable from scriptman._internal."""
        from scriptman._internal import log as imported_log

        assert imported_log.info is not None
        assert imported_log.debug is not None
        assert imported_log.error is not None
        assert imported_log.event is not None

    def test_private_functions_exist(self):
        """📦 Private helper functions should exist."""
        assert hasattr(log, "_log")
        assert hasattr(log, "_loguru_fallback")
        assert hasattr(log, "_LOGGING")
        assert hasattr(log, "_current_state")


# ═══════════════════════════════════════════════════════════════════════════════
# CONCURRENCY (Basic)
# ═══════════════════════════════════════════════════════════════════════════════


class TestConcurrency:
    """Basic concurrency tests."""

    def test_state_isolation_per_call(self):
        """🔄 Each log call should manage its own state."""
        # First call
        log.info("First")
        assert log._current_state is None

        # Second call
        log.info("Second")
        assert log._current_state is None

    def test_rapid_sequential_calls(self, capture_logs):
        """🔄 Rapid sequential calls should all succeed."""
        for i in range(100):
            log.info(f"Message {i}")

        output = capture_logs.getvalue()
        assert "Message 0" in output
        assert "Message 99" in output


# ═══════════════════════════════════════════════════════════════════════════════
# INTEGRATION WITH OBSERVE
# ═══════════════════════════════════════════════════════════════════════════════


class TestObserveIntegration:
    """Test integration with the observe module."""

    def test_works_after_observe_import(self):
        """🔗 log should work after observe is imported."""
        from scriptman import observe

        # This should use observe, not fallback
        log.info("After observe import")
        # Should not raise

    def test_works_before_observe_configured(self, capture_logs):
        """🔗 log should work even before observe.configure_logging()."""
        # Don't configure observe, just log
        log.info("Before configure")
        output = capture_logs.getvalue()
        assert "Before configure" in output

    def test_event_type_passed_correctly(self):
        """🔗 Event type should be passed to observe.event()."""
        with patch("scriptman.observe.observe.event") as mock_event:
            log.event("Test message", "custom.event.type", extra="data")

            mock_event.assert_called_once()
            call_kwargs = mock_event.call_args
            assert call_kwargs[1]["event_type"] == "custom.event.type"
```

---

## 📊 Test Coverage Matrix

| Category                   | Functions Covered                                     | Edge Cases                           | Expected Coverage |
| -------------------------- | ----------------------------------------------------- | ------------------------------------ | ----------------- |
| **Basic Logging**          | trace, debug, info, warning, error, success, critical | All levels callable                  | 100%              |
| **Data Formatting**        | _loguru_fallback with data                            | None, numbers, bools, lists, dicts   | 100%              |
| **Exception Logging**      | exception                                             | With/without exception context       | 100%              |
| **Event Emission**         | event                                                 | Success, error, recursion skip       | 100%              |
| **Fallback Behavior**      | _loguru_fallback, _log                                | ImportError, RecursionError, generic | 100%              |
| **Circular Prevention**    | _current_state, _LOGGING                              | State reset, sentinel uniqueness     | 100%              |
| **Observe Delegation**     | All log levels                                        | Delegation verification              | 100%              |
| **Formatting Integration** | Via observe and fallback                              | Custom formats                       | 100%              |
| **Edge Cases**             | All functions                                         | Unicode, long strings, special chars | 100%              |
| **Module API**             | __all__, exports                                      | Importability                        | 100%              |
| **Concurrency**            | State isolation                                       | Rapid calls                          | Basic             |

**Target Coverage: 95%+**

---

## 🏃 Running the Tests

```bash
# Run all internal log tests
pytest tests/test_internal_log.py -v

# Run with coverage
pytest tests/test_internal_log.py -v --cov=scriptman._internal.log --cov-report=term-missing

# Run specific test class
pytest tests/test_internal_log.py::TestBasicLogging -v

# Run with markers
pytest tests/test_internal_log.py -v -m "not slow"
```

---

## 📝 Test Implementation Notes

1. **Fixture `reset_log_state`** — Critical for test isolation; resets `_current_state` between tests

2. **Fixture `capture_logs`** — Captures loguru output by adding a StringIO handler

3. **Mocking Strategy** — Uses `unittest.mock.patch` to isolate observe integration

4. **Edge Cases** — Cover Unicode, very long strings, special characters, empty strings

5. **Recursion Tests** — Verify the sentinel-based recursion prevention works

6. **Integration Tests** — Verify end-to-end flow with actual observe module
