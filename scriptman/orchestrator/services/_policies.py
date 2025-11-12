from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from typing_extensions import Literal

from scriptman.orchestrator.workloads import WorkloadEventPayload


@dataclass(slots=True)
class BackoffConfig:
    """⏱️ Configure restart backoff behaviour."""

    initial_delay: float = 1.0
    multiplier: float = 2.0
    max_delay: float = 60.0


RestartStrategy = Literal["never", "always", "limited"]


@dataclass(slots=True)
class RestartPolicy:
    """♻️ Control how services recover from failures."""

    strategy: RestartStrategy = "always"
    max_retries: int | None = None
    backoff: BackoffConfig = BackoffConfig()
    reset_timeout: float = 300.0
    on_failure: Callable[[WorkloadEventPayload], None] | None = (
        None  # hook receives failure payload
    )

    @classmethod
    def never(cls) -> "RestartPolicy":
        return cls(strategy="never", max_retries=0)

    @classmethod
    def always(cls) -> "RestartPolicy":
        return cls(strategy="always", max_retries=None)

    @classmethod
    def limited(
        cls,
        *,
        max_retries: int,
        backoff: BackoffConfig | None = None,
        reset_timeout: float = 300.0,
    ) -> "RestartPolicy":
        return cls(
            strategy="limited",
            max_retries=max_retries,
            backoff=backoff or BackoffConfig(),
            reset_timeout=reset_timeout,
        )


__all__ = ["BackoffConfig", "RestartPolicy", "RestartStrategy"]
