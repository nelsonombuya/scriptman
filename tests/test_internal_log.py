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
        with patch("scriptman.observe.observe.event") as mock_event:
            log.event("Test event", "test.type", key="value")
            mock_event.assert_called_once()
            call_args = mock_event.call_args
            assert call_args[0][0] == "Test event"
            assert call_args[1]["event_type"] == "test.type"
            assert call_args[1]["key"] == "value"

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

    def test_falls_back_on_recursion_detection(self, capture_logs):
        """🔄 Should fall back to loguru when recursion detected."""
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

    def test_state_is_none_initially(self):
        """🔄 _current_state should be None when not logging."""
        # After reset fixture runs, state should be None
        assert log._current_state is None


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

    def test_delegates_trace_to_observe(self):
        """🔍 log.trace() should call observe.trace()."""
        with patch("scriptman.observe.observe.trace") as mock_trace:
            log.trace("Trace message")
            mock_trace.assert_called_once_with("Trace message")

    def test_delegates_success_to_observe(self):
        """🔍 log.success() should call observe.success()."""
        with patch("scriptman.observe.observe.success") as mock_success:
            log.success("Success message")
            mock_success.assert_called_once_with("Success message")

    def test_falls_back_for_unknown_level(self, capture_logs):
        """🔍 Unknown level should fall back to info via getattr."""
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
        """🎨 Internal log should use configured format via loguru."""
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
        # Should have time format (colon separator for time)
        assert ":" in result

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
        assert "INFO" in output

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

    def test_bytes_in_data(self, capture_logs):
        """📝 Bytes in data should be handled."""
        log.info("Test", data=b"binary data")
        output = capture_logs.getvalue()
        assert "data=" in output

    def test_set_in_data(self, capture_logs):
        """📝 Set in data should be handled."""
        log.info("Test", items={1, 2, 3})
        output = capture_logs.getvalue()
        assert "items=" in output

    def test_tuple_in_data(self, capture_logs):
        """📝 Tuple in data should be handled."""
        log.info("Test", coords=(10, 20))
        output = capture_logs.getvalue()
        assert "coords=" in output


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE API
# ═══════════════════════════════════════════════════════════════════════════════


class TestModuleAPI:
    """Test module exports and API surface."""

    def test_all_exports_exist(self):
        """📦 All __all__ exports should exist as attributes."""
        expected_exports = [
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
            call_kwargs = mock_event.call_args[1]
            assert call_kwargs["event_type"] == "custom.event.type"
            assert call_kwargs["extra"] == "data"


# ═══════════════════════════════════════════════════════════════════════════════
# IMPORT ERROR HANDLING
# ═══════════════════════════════════════════════════════════════════════════════


class TestImportErrorHandling:
    """Test behavior when imports fail."""

    def test_handles_import_error_gracefully(self, capture_logs):
        """🔄 Should handle ImportError and use fallback."""
        # We can't easily simulate ImportError for scriptman.observe
        # since it's already imported, but we can test the fallback path
        log._loguru_fallback("info", "Import error fallback")
        output = capture_logs.getvalue()
        assert "Import error fallback" in output

    def test_handles_recursion_error(self, capture_logs):
        """🔄 Should handle RecursionError and use fallback."""
        # Simulate recursion state
        log._current_state = log._LOGGING
        log._log("info", "Recursion handled")
        output = capture_logs.getvalue()
        assert "Recursion handled" in output


# ═══════════════════════════════════════════════════════════════════════════════
# SUBPROCESS CIRCULAR IMPORT TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestCircularImportIntegration:
    """Test circular import prevention via subprocess isolation."""

    def test_import_config_standalone(self):
        """🔗 Importing config alone should not cause circular import."""
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-c", "from scriptman import config; print('OK')"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, f"Import failed: {result.stderr}"
        assert "OK" in result.stdout

    def test_import_observe_standalone(self):
        """🔗 Importing observe alone should not cause circular import."""
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-c", "from scriptman import observe; print('OK')"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, f"Import failed: {result.stderr}"
        assert "OK" in result.stdout

    def test_import_cache_standalone(self):
        """🔗 Importing cache alone should not cause circular import."""
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-c", "from scriptman import cache; print('OK')"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, f"Import failed: {result.stderr}"
        assert "OK" in result.stdout

    def test_import_database_standalone(self):
        """🔗 Importing database alone should not cause circular import."""
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-c", "from scriptman import database; print('OK')"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, f"Import failed: {result.stderr}"
        assert "OK" in result.stdout

    def test_import_internal_log_standalone(self):
        """🔗 Importing _internal.log should not cause circular import."""
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-c", "from scriptman._internal import log; print('OK')"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, f"Import failed: {result.stderr}"
        assert "OK" in result.stdout

    def test_import_order_independence(self):
        """🔗 Import order should not affect functionality."""
        import subprocess
        import sys

        # Import in different order than normal
        code = """
from scriptman._internal import log
from scriptman import observe
from scriptman import config
from scriptman import cache
log.info("Test from internal log")
observe.info("Test from observe")
print('OK')
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, f"Import failed: {result.stderr}"
        assert "OK" in result.stdout


# ═══════════════════════════════════════════════════════════════════════════════
# SERIALIZATION IN FALLBACK
# ═══════════════════════════════════════════════════════════════════════════════


class TestFallbackSerialization:
    """Test that fallback path handles serialization correctly."""

    def test_path_objects_serialized(self, capture_logs):
        """📁 Path objects should be serialized in fallback."""
        from pathlib import Path

        log._loguru_fallback("info", "Path test", path=Path(".data/logs"))
        output = capture_logs.getvalue()
        assert "Path test" in output
        assert "path=" in output
        # Should be serialized to string, not PosixPath(...) or WindowsPath(...)
        # Accept both Unix (/) and Windows (\) separators
        assert ".data/logs" in output or ".data\\logs" in output

    def test_complex_objects_serialized(self, capture_logs):
        """📦 Complex objects should be serialized in fallback."""
        log._loguru_fallback(
            "info",
            "Complex test",
            data={"nested": {"key": "value"}},
            items=[1, 2, 3],
        )
        output = capture_logs.getvalue()
        assert "Complex test" in output
        assert "data=" in output
        assert "items=" in output
