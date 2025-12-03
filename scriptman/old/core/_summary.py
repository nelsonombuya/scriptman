from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from json import dump, load
from pathlib import Path
from typing import Any, Iterable, Optional

from loguru import logger

from scriptman.core.config import config


@dataclass(slots=True)
class ScriptRunRecord:
    """📊 Normalized record describing a single script execution."""

    script_path: str
    started_at: datetime
    ended_at: datetime
    duration_seconds: float
    attempts: int
    retries_configured: int
    success: bool
    error_type: str | None
    error_message: str | None
    log_path: str | None
    summary_path: str | None
    host: str


@dataclass(slots=True)
class ServiceRunRecord:
    """📊 Normalized record describing a single service iteration."""

    service_name: str
    started_at: datetime
    ended_at: datetime
    duration_seconds: float
    success: bool
    error_type: str | None
    error_message: str | None
    log_path: str | None
    summary_path: str | None
    host: str


class JobSummary:
    """📘 Aggregated summary for scripts executed in the current session."""

    def __init__(self) -> None:
        self._records: list[ScriptRunRecord] = []
        self._session_started_at: Optional[datetime] = None
        self._session_finished_at: Optional[datetime] = None

    def start_session(self) -> None:
        """🚀 Begin tracking a new execution session."""
        self._records.clear()
        self._session_started_at = datetime.now()
        self._session_finished_at = None

    def add_record(self, record: ScriptRunRecord) -> None:
        """✍️ Append a script execution record to the current session."""
        self._records.append(record)

    def end_session(self) -> None:
        """🧹 Mark the session as finished."""
        self._session_finished_at = datetime.now()

    @property
    def records(self) -> Iterable[ScriptRunRecord]:
        """🔍 Expose stored execution records."""
        return tuple(self._records)

    def as_payload(self) -> dict[str, Any]:
        """📦 Convert the session summary into a serializable payload."""
        successful = [record for record in self._records if record.success]
        failed = [record for record in self._records if not record.success]

        return {
            "session_start": (
                self._session_started_at.isoformat() if self._session_started_at else None
            ),
            "session_end": (
                self._session_finished_at.isoformat()
                if self._session_finished_at
                else None
            ),
            "total_runs": len(self._records),
            "successful_runs": len(successful),
            "failed_runs": len(failed),
            "records": [asdict(record) for record in self._records],
        }

    def save_to_file(self, file_path: Path) -> None:
        """💾 Persist the aggregated session summary to disk."""
        file_path.write_text(
            dump_json(self.as_payload()),
            encoding="utf-8",
        )


def dump_json(payload: Any) -> str:
    """🔍 Serialize payload to pretty JSON."""
    from json import dumps

    return dumps(payload, indent=2, default=str)


class JobSummaryService:
    """📊 Aggregates service and scheduled job executions by day."""

    __instance: Optional["JobSummaryService"] = None
    __initialized: bool = False

    def __new__(cls, *args: Any, **kwargs: Any) -> "JobSummaryService":
        if cls.__instance is None:
            cls.__instance = super(JobSummaryService, cls).__new__(cls, *args, **kwargs)
            cls.__instance.__initialized = False
        return cls.__instance

    def __init__(self) -> None:
        if not self.__initialized:
            self._summaries: dict[str, dict[str, Any]] = {}
            self._load_existing()
            self.__initialized = True

    def _load_existing(self) -> None:
        logs_root = Path(config.settings.get("logs_dir", "logs"))
        summary_dir = logs_root / "services" / "_daily"
        if not summary_dir.exists():
            summary_dir.mkdir(parents=True, exist_ok=True)
            return

        for summary_file in summary_dir.glob("*.json"):
            try:
                with summary_file.open("r", encoding="utf-8") as handle:
                    data = load(handle)
                    key = summary_file.stem
                    self._summaries[key] = data
            except Exception as exc:  # noqa: BLE001
                logger.error(f"⚠️ Error loading summary file {summary_file}: {exc}")

    def _persist_day(self, day: str) -> None:
        logs_root = Path(config.settings.get("logs_dir", "logs"))
        summary_dir = logs_root / "services" / "_daily"
        summary_dir.mkdir(parents=True, exist_ok=True)
        target = summary_dir / f"{day}.json"
        try:
            with target.open("w", encoding="utf-8") as handle:
                dump(self._summaries[day], handle, indent=2, default=str)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"⚠️ Error saving summary file {target}: {exc}")

    def add_service_run(self, record: ServiceRunRecord) -> None:
        """✍️ Append a service runtime record to the daily ledger."""
        day = record.started_at.date().isoformat()

        daily = self._summaries.setdefault(
            day,
            {
                "date": day,
                "total_runs": 0,
                "successful_runs": 0,
                "failed_runs": 0,
                "records": [],
            },
        )

        daily["records"].append(asdict(record))
        daily["total_runs"] += 1
        if record.success:
            daily["successful_runs"] += 1
        else:
            daily["failed_runs"] += 1

        self._persist_day(day)

    def get_summary(self, day: Optional[str] = None) -> Optional[dict[str, Any]]:
        """🔍 Retrieve the aggregated summary for ``day`` (defaults to today)."""
        day = day or datetime.now().date().isoformat()
        return self._summaries.get(day)

    def cleanup_old_summaries(self, days_to_keep: int = 30) -> None:
        """🧹 Remove daily summaries older than ``days_to_keep`` days."""
        cutoff = datetime.now().date() - timedelta(days=days_to_keep)
        logs_root = Path(config.settings.get("logs_dir", "logs"))
        summary_dir = logs_root / "services" / "_daily"

        for day, payload in list(self._summaries.items()):
            if datetime.fromisoformat(day).date() >= cutoff:
                continue
            target = summary_dir / f"{day}.json"
            try:
                if target.exists():
                    target.unlink()
            except Exception as exc:  # noqa: BLE001
                logger.error(f"⚠️ Error removing summary file {target}: {exc}")
            self._summaries.pop(day, None)
