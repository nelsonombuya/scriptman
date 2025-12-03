"""🚀 Script execution configuration schema."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ExecutionConfig(BaseModel):
    """🚀 Script execution configuration.

    Controls how scripts are executed by Scriptman.

    Attributes:
        concurrent: Whether to run scripts concurrently when possible

    Example:
        >>> config.get("execution.concurrent")
        True
    """

    concurrent: bool = Field(
        default=True,
        description="Run scripts concurrently when possible",
    )
