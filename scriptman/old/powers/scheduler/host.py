"""🪢 Protocols describing the scheduler's host interactions."""

from __future__ import annotations

from typing import Any, Callable, Protocol

from scriptman.powers.service import ServiceCallable


class SchedulerHost(Protocol):
    """🧭 Contract implemented by ``TaskManager`` for scheduler cooperation."""

    def background(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """⚙️ Submit a callable to the task execution layer.

        Args:
            func: Callable that performs the work.
            *args: Positional arguments supplied to ``func``.
            **kwargs: Keyword arguments supplied to ``func``.

        Returns:
            Task handle or Future-like object mirroring the TaskManager API.
        """

        ...

    def register_service(
        self,
        name: str,
        target: ServiceCallable,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """🧾 Register a long-running service loop.

        Args:
            name: Unique service identifier.
            target: Function executed by the service thread.
            *args: Additional positional parameters forwarded to registration.
            **kwargs: Additional keyword parameters forwarded to registration.
        """

        ...

    def start_service(self, name: str) -> None:
        """▶️ Start a named service if it is registered.

        Args:
            name: Identifier of the service to start.
        """

        ...

    def stop_service(self, name: str, *, timeout: float | None = None) -> None:
        """⏹ Stop a named service, optionally waiting for shutdown.

        Args:
            name: Identifier of the service to stop.
            timeout: Optional seconds to wait for graceful termination.
        """

        ...

    def has_running_services(self) -> bool:
        """🧐 Report whether any services are currently active.

        Returns:
            ``True`` when at least one service thread is alive, ``False`` otherwise.
        """

        ...


__all__ = ["SchedulerHost"]
