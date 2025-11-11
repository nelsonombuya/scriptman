from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, DefaultDict, Iterable

from scriptman.core._summary import ScriptRunRecord

SuccessCallback = Callable[[ScriptRunRecord], None]
FailureCallback = Callable[[ScriptRunRecord, Exception], None]


@dataclass(slots=True)
class ScriptCallbackRegistry:
    """🔁 Registry that dispatches script success/failure callbacks.

    Args:
        _success: List of success callbacks.
        _failure: List of failure callbacks.
        _error_specific: Dictionary of exception-specific failure callbacks.
    """

    _success: list[SuccessCallback] = field(default_factory=list)
    _failure: list[FailureCallback] = field(default_factory=list)
    _error_specific: DefaultDict[type[BaseException], list[FailureCallback]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def register_success_callback(self, callback: SuccessCallback) -> None:
        """✍️ Register a success callback executed after successful script runs.

        Args:
            callback: The success callback to register.
        """
        self._success.append(callback)

    def register_failure_callback(
        self,
        callback: FailureCallback,
        *,
        exception: type[BaseException] | None = None,
    ) -> None:
        """✍️ Register a failure callback executed after failed script runs.

        Args:
            callback: Invoked with the run record and raised exception.
            exception: Optional exception type to scope the callback to.
                When omitted, the callback triggers for all failures.
        """
        if exception is None:
            self._failure.append(callback)
            return
        self._error_specific[exception].append(callback)

    def remove_success_callback(self, callback: SuccessCallback) -> None:
        """🧹 Remove a previously registered success callback.

        Args:
            callback: The success callback to remove.
        """
        if callback in self._success:
            self._success.remove(callback)

    def remove_failure_callback(
        self,
        callback: FailureCallback,
        *,
        exception: type[BaseException] | None = None,
    ) -> None:
        """🧹 Remove a previously registered failure callback.

        Args:
            callback: The failure callback to remove.
            exception: Optional exception type to scope the callback to.
                When omitted, the callback triggers for all failures.
        """
        if exception is None:
            if callback in self._failure:
                self._failure.remove(callback)
            return

        callbacks = self._error_specific.get(exception)
        if callbacks and callback in callbacks:
            callbacks.remove(callback)
            if not callbacks:
                self._error_specific.pop(exception, None)

    def dispatch_success_callbacks(self, record: ScriptRunRecord) -> None:
        """📣 Execute success callbacks.

        Args:
            record: The script run record.
        """
        for callback in list(self._success):
            try:
                callback(record)
            except Exception:  # noqa: BLE001 - callbacks must not crash execution
                continue

    def dispatch_failure_callbacks(
        self, record: ScriptRunRecord, error: Exception
    ) -> None:
        """📣 Execute failure callbacks for ``error``.

        Args:
            record: The script run record.
            error: The exception that occurred.
        """
        for callback in list(self._failure):
            try:
                callback(record, error)
            except Exception:  # noqa: BLE001 - callbacks must not crash execution
                continue

        for exc_type, callbacks in list(self._error_specific.items()):
            if isinstance(error, exc_type):
                for callback in list(callbacks):
                    try:
                        callback(record, error)
                    except Exception:  # noqa: BLE001
                        continue


DEFAULT_SCRIPT_CALLBACKS = ScriptCallbackRegistry()


def success_callbacks() -> Iterable[SuccessCallback]:
    """🔍 Inspect globally registered success callbacks."""
    return tuple(DEFAULT_SCRIPT_CALLBACKS._success)


def failure_callbacks() -> Iterable[FailureCallback]:
    """🔍 Inspect globally registered failure callbacks."""
    return tuple(DEFAULT_SCRIPT_CALLBACKS._failure)


__all__ = [
    "ScriptCallbackRegistry",
    "DEFAULT_SCRIPT_CALLBACKS",
    "success_callbacks",
    "failure_callbacks",
]
