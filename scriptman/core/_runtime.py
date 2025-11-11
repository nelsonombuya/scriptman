from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, Literal

from loguru import logger

from scriptman.core.config import config

RuntimeKind = Literal["scripts", "services"]


@dataclass(slots=True)
class RuntimeArtifacts:
    """📁 Runtime log artifacts generated for scripts or services.

    Args:
        log_path: Path to the log file capturing runtime output.
        summary_path: Path to the JSON summary generated for the run.
        timestamp: Timestamp used when the run artifacts were created.
    """

    log_path: Path
    summary_path: Path
    timestamp: datetime


@contextmanager
def runtime_artifacts(
    kind: RuntimeKind,
    name: str,
    *,
    log_level: str | None = None,
) -> Iterator[RuntimeArtifacts]:
    """🗂️ Create scoped runtime artifacts for scripts and services.

    Args:
        kind: Artifact namespace, typically ``\"scripts\"`` or ``\"services\"``.
        name: Logical name for the script or service (used as folder name).
        log_level: Optional override for the log level used by the log sink.

    Yields:
        RuntimeArtifacts describing generated file locations.
    """

    logs_dir = Path(config.settings.get("logs_dir", "logs"))
    target_dir = logs_dir / kind / name
    target_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now()
    slug = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
    log_path = target_dir / f"{slug}.log"
    summary_path = target_dir / f"{slug}.json"

    handler_id = logger.add(
        log_path,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
        level=log_level or str(config.settings.get("log_level", "INFO")),
    )

    try:
        yield RuntimeArtifacts(
            log_path=log_path, summary_path=summary_path, timestamp=timestamp
        )
    finally:
        logger.remove(handler_id)
