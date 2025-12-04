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
    """📝 Direct loguru output when observe isn't available.

    This function is the last-resort fallback during early initialization
    or when observe fails. It must never raise exceptions.

    Args:
        level: Log level name (trace, debug, info, warning, error, critical)
        message: Log message
        **data: Additional data to format as key=value pairs

    Note:
        Unknown log levels default to INFO. Data formatting uses serialize()
        when available, falling back to str() during very early initialization.
    """
    log_method = getattr(logger, level.lower(), logger.info)

    # Format data for console (serialize for consistency with observe)
    if data:
        try:
            from scriptman.serialization import serialize

            data_str = " | " + " ".join(f"{k}={serialize(v)}" for k, v in data.items())
        except ImportError:
            # Serialization not available during early init — use str()
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

    Returns:
        None. Events are fire-and-forget; failures are silently ignored
        to prevent logging from disrupting application flow.

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
