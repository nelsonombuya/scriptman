from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

from loguru import logger

DEFAULT_PATH_TEMPLATE = "logs/{workload}/{date}/{run_id}.log"


@dataclass(slots=True)
class WorkloadLoggingOptions:
    """🗂️ Configuration for per-run workload logging."""

    enabled: bool = True
    include_args: bool = False
    path_template: str = DEFAULT_PATH_TEMPLATE
    extra: Mapping[str, object] = field(default_factory=dict)


def configure_logging(
    *,
    enabled: bool | None = None,
    path_template: str | None = None,
    include_args: bool | None = None,
    extra: Mapping[str, object] | None = None,
) -> WorkloadLoggingOptions:
    options = WorkloadLoggingOptions()
    if enabled is not None:
        options.enabled = enabled
    if path_template is not None:
        options.path_template = path_template
    if include_args is not None:
        options.include_args = include_args
    if extra is not None:
        options.extra = extra
    return options


def log_to(
    path_template: str | None = None,
    *,
    enabled: bool = True,
    include_args: bool = False,
    extra: Mapping[str, object] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorate a callable to customise per-run logging behaviour."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        options = WorkloadLoggingOptions(
            enabled=enabled,
            path_template=path_template or DEFAULT_PATH_TEMPLATE,
            include_args=include_args,
            extra=extra or {},
        )
        setattr(func, "__workload_log_options__", options)
        return func

    return decorator


def extract_logging_options(func: Callable[..., Any]) -> WorkloadLoggingOptions:
    options = getattr(func, "__workload_log_options__", None)
    if isinstance(options, WorkloadLoggingOptions):
        return options
    return WorkloadLoggingOptions()


@contextmanager
def workload_log_sink(
    *,
    workload: str,
    run_id: str,
    options: WorkloadLoggingOptions,
    args: tuple[Any, ...] | None = None,
    kwargs: Mapping[str, Any] | None = None,
) -> Iterator[None]:
    """Context manager that binds per-run logging sinks and context."""

    if not options.enabled:
        with logger.contextualize(workload=workload, run_id=run_id):
            yield
        return

    now = datetime.now(tz=timezone.utc)
    timestamp = now.strftime("%Y%m%d-%H%M%S")
    path = options.path_template.format(
        workload=workload,
        run_id=run_id,
        timestamp=timestamp,
        date=now.strftime("%Y-%m-%d"),
    )
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    context: dict[str, object] = {
        "workload": workload,
        "run_id": run_id,
    }
    if options.include_args:
        context["args"] = args or ()
        context["kwargs"] = kwargs or {}
    context.update(options.extra)

    sink_id = logger.add(destination, enqueue=True)
    with logger.contextualize(**context):
        try:
            yield
        finally:
            logger.remove(sink_id)


__all__ = [
    "WorkloadLoggingOptions",
    "configure_logging",
    "extract_logging_options",
    "log_to",
    "workload_log_sink",
]
