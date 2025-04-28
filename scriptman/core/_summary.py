from datetime import date, datetime, timedelta
from json import dump, load
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from scriptman.core.config import config


class JobSummary:
    def __init__(self) -> None:
        """
        ✨ Initializes the JobSummary class to track script executions.
        """
        self.jobs: list[dict[str, Any]] = []
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

    def start_session(self) -> None:
        """Start tracking a new session of jobs."""
        self.start_time = datetime.now()
        self.jobs = []

    def add_job(
        self, script_path: Path | str, success: bool, error: Optional[Exception] = None
    ) -> None:
        """Add a job execution to the summary."""
        job = {
            "script_path": str(script_path),
            "timestamp": datetime.now().isoformat(),
            "success": success,
            "error": str(error) if error else None,
        }
        self.jobs.append(job)

    def end_session(self) -> None:
        """End the current session."""
        self.end_time = datetime.now()

    def to_json(self) -> str:
        """Convert the summary to JSON format."""
        from json import dumps

        summary = {
            "session_start": self.start_time.isoformat() if self.start_time else None,
            "session_end": self.end_time.isoformat() if self.end_time else None,
            "total_jobs": len(self.jobs),
            "successful_jobs": sum(1 for job in self.jobs if job["success"]),
            "failed_jobs": sum(1 for job in self.jobs if not job["success"]),
            "jobs": self.jobs,
        }
        return dumps(summary, indent=2)

    def save_to_file(self, file_path: Path) -> None:
        """Save the summary to a JSON file."""
        with open(file_path, "w") as f:
            f.write(self.to_json())


class JobSummaryService:
    _instance: Optional["JobSummaryService"] = None
    _initialized: bool = False

    def __new__(cls, *args: Any, **kwargs: Any) -> "JobSummaryService":
        if cls._instance is None:
            cls._instance = super(JobSummaryService, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        if not self._initialized:
            self._current_date = datetime.now().date().isoformat()
            self._summaries: dict[str, dict[str, Any]] = {}
            self._load_existing_summaries()
            self._initialized = True

    def _load_existing_summaries(self) -> None:
        """Load existing summaries from JSON files."""
        logs_dir = Path(config.settings.logs_dir)
        if not logs_dir.exists():
            logs_dir.mkdir(parents=True)
            return

        for summary_file in logs_dir.glob("scriptman_summary_*.json"):
            try:
                with open(summary_file, "r") as f:
                    date_str = summary_file.stem.split("_")[-1]
                    self._summaries[date_str] = load(f)
            except Exception as e:
                logger.error(f"Error loading summary file {summary_file}: {e}")

    def add_job(
        self, job_id: str, job_name: str, success: bool, error: Optional[Exception] = None
    ) -> None:
        """Add a job execution to the current day's summary."""
        today = date.today().isoformat()
        if today not in self._summaries:
            self._summaries[today] = {
                "date": today,
                "total_jobs": 0,
                "successful_jobs": 0,
                "failed_jobs": 0,
                "jobs": [],
            }

        job_entry = {
            "job_id": job_id,
            "job_name": job_name,
            "timestamp": datetime.now().isoformat(),
            "success": success,
            "error": str(error) if error else None,
        }

        self._summaries[today]["jobs"].append(job_entry)
        self._summaries[today]["total_jobs"] += 1
        if success:
            self._summaries[today]["successful_jobs"] += 1
        else:
            self._summaries[today]["failed_jobs"] += 1

        self._save_summary(today)

    def _save_summary(self, date_str: str) -> None:
        """Save the summary for a specific date to a JSON file."""
        if date_str not in self._summaries:
            return

        _file = Path(config.settings.logs_dir) / f"scriptman_summary_{date_str}.json"
        try:
            with open(_file, "w") as f:
                dump(self._summaries[date_str], f, indent=2)
        except Exception as e:
            logger.error(f"Error saving summary file {_file}: {e}")

    def get_summary(self, date_str: Optional[str] = None) -> Optional[dict[str, Any]]:
        """Get the summary for a specific date or today if no date is provided."""
        if date_str is None:
            date_str = datetime.now().date().isoformat()
        return self._summaries.get(date_str)

    def cleanup_old_summaries(self, days_to_keep: int = 30) -> None:
        """Remove summaries older than the specified number of days."""
        cutoff_date = datetime.now().date() - timedelta(days=days_to_keep)
        dates_to_remove = [
            date_str
            for date_str in self._summaries
            if datetime.fromisoformat(date_str).date() < cutoff_date
        ]

        for date_str in dates_to_remove:
            summary_file = Path(".logs") / f"scriptman_summary_{date_str}.json"
            try:
                summary_file.unlink()
            except Exception as e:
                logger.error(f"Error removing summary file {summary_file}: {e}")
            del self._summaries[date_str]
