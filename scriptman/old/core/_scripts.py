import sys
from asyncio import gather, run, to_thread
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from platform import node
from runpy import run_path
from time import sleep
from typing import Optional, Tuple

from filelock import FileLock, Timeout
from loguru import logger

from scriptman.core._callbacks import DEFAULT_SCRIPT_CALLBACKS, ScriptCallbackRegistry
from scriptman.core._runtime import runtime_artifacts
from scriptman.core._summary import JobSummary, ScriptRunRecord, dump_json
from scriptman.core.config import config
from scriptman.powers.time_calculator import TimeCalculator


@dataclass(slots=True)
class ScriptExecutionOptions:
    """⚙️ Tunable options applied during script execution."""

    retries: int = 0
    force: bool = False
    verbose: bool = False
    log_level: str | None = None
    script_args: list[str] = field(default_factory=list)

    @classmethod
    def from_config(cls) -> "ScriptExecutionOptions":
        """🔍 Build options based on current configuration defaults."""
        return cls(
            retries=int(config.settings.get("retries", 0)),
            force=bool(config.settings.get("force", False)),
            verbose=bool(config.settings.get("verbose", False)),
            log_level=str(config.settings.get("log_level", "INFO")),
            script_args=list(config.settings.get("script_args", [])),
        )


class Scripts:
    def __init__(
        self,
        *,
        options: Optional[ScriptExecutionOptions] = None,
        callbacks: Optional[ScriptCallbackRegistry] = None,
    ) -> None:
        """
        ✨ Initializes the Scripts class with an empty dictionary to store
        script execution results and a job summary tracker.
        """
        self._default_options = options or ScriptExecutionOptions.from_config()
        self._default_callbacks = callbacks or DEFAULT_SCRIPT_CALLBACKS
        self.__results: dict[Path, ScriptRunRecord] = {}
        self.job_summary = JobSummary()
        self.job_summary.start_session()

    def run_scripts(
        self,
        scripts: list[Path],
        *,
        options: Optional[ScriptExecutionOptions] = None,
        callbacks: Optional[ScriptCallbackRegistry] = None,
    ) -> None:
        """
        🏃‍♂️ Executes a list of scripts either concurrently or sequentially based on the
        configuration.

        Args:
            scripts (list[Path]): A list of script paths to be executed.
            options: Optional overrides applied for this run only.
            callbacks: Optional callback registry for success/failure notifications.
        """
        execution_options = options or self._default_options
        callback_registry = callbacks or self._default_callbacks
        normalized_scripts = self._normalize_scripts(scripts)

        with TimeCalculator.context("Scriptman"):
            if not normalized_scripts:
                logger.warning("🔍 No scripts to run")
                self.job_summary.end_session()
                return

            if config.settings.get("concurrent", True) and len(normalized_scripts) > 1:
                self.__execute_scripts_concurrently(
                    normalized_scripts, execution_options, callback_registry
                )
            else:
                self.__execute_scripts_sequentially(
                    normalized_scripts, execution_options, callback_registry
                )

            summary_lines = "\n".join(
                self.__format_result(record) for record in self.__results.values()
            )
            logger.info(
                "✅ Script batch completed\n"
                "--------------------------------------------------\n"
                f"{summary_lines}\n"
                "--------------------------------------------------"
            )

        self.save_summary()

    def __format_result(self, record: ScriptRunRecord) -> str:
        """
        ✍🏾 Formats the result of a single script execution into a string.

        Args:
            record: The script run record.

        Returns:
            str: A string that represents the status of the script execution.
        """
        if record.success:
            status = "✅ Success"
        else:
            status = (
                f"❌ {record.error_type}: {record.error_message}"
                if record.error_type
                else "❌ Failed"
            )
        return (
            f"💡 {Path(record.script_path).name} — {status} "
            f"({record.duration_seconds:.2f}s)"
        )

    def __execute_scripts_sequentially(
        self,
        scripts: list[Path],
        options: ScriptExecutionOptions,
        callbacks: ScriptCallbackRegistry,
    ) -> None:
        """
        🏃🏾‍♂️🏃🏾‍♂️ Executes a list of scripts sequentially.

        Args:
            scripts (list[Path]): A list of Path objects representing the scripts to be
                executed.

        Returns:
            dict[Path, Any]: A dictionary mapping each script to its execution result.
        """
        for script in scripts:
            self.__lock_and_load_script(script, options, callbacks)

    def __execute_scripts_concurrently(
        self,
        scripts: list[Path],
        options: ScriptExecutionOptions,
        callbacks: ScriptCallbackRegistry,
    ) -> None:
        """
        🛤 Executes a list of scripts concurrently.

        This method runs all the provided scripts asynchronously using asyncio.

        Args:
            scripts (list[Path]): A list of Path objects representing the scripts to be
                executed.
        """

        async def __run_all_scripts_async(scripts: list[Path]) -> None:
            await gather(
                *(
                    to_thread(self.__lock_and_load_script, s, options, callbacks)
                    for s in scripts
                )
            )

        run(__run_all_scripts_async(scripts))

    def __lock_and_load_script(
        self,
        script: Path,
        options: ScriptExecutionOptions,
        callbacks: ScriptCallbackRegistry,
    ) -> None:
        """🔫 Lock and load script for execution."""
        lock = FileLock(script.with_suffix(script.suffix + ".lock"), timeout=0)

        try:
            if not options.force:
                logger.debug(f"🔐 Acquiring lock for '{script.name}'...")
                with lock:
                    logger.debug(f"🔒 Lock acquired for '{script.name}'.")
                    record, error = self._execute_script(script, options, callbacks)
            else:
                logger.warning(f"⚠ Force flag set. Skipping lock for '{script.name}'.")
                record, error = self._execute_script(script, options, callbacks)

            self.__results[script] = record
        except Timeout as e:
            logger.error(
                f"Another instance of '{script.name}' is already running. "
                "Run using --force to override."
            )
            record = ScriptRunRecord(
                script_path=str(script),
                started_at=datetime.now(),
                ended_at=datetime.now(),
                duration_seconds=0.0,
                attempts=0,
                retries_configured=options.retries,
                success=False,
                error_type=type(e).__name__,
                error_message=str(e),
                log_path=None,
                summary_path=None,
                host=node() or "unknown",
            )
            callbacks.dispatch_failure_callbacks(record, e)
            self.job_summary.add_record(record)
            self.__results[script] = record
        finally:
            logger.debug(f"🔓 Releasing lock for '{script.name}'.")
            lock.release()

    def _execute_script(
        self,
        file_path: Path,
        options: ScriptExecutionOptions,
        callbacks: ScriptCallbackRegistry,
    ) -> Tuple[ScriptRunRecord, Optional[Exception]]:
        """🔍 Execute a script and return the record and any exception."""
        host = node() or "unknown"
        started_at = datetime.now()
        verbose_handler: Optional[int] = None

        if options.verbose:
            verbose_handler = logger.add(
                lambda msg: print(msg), level="DEBUG", format="{message}"
            )

        with runtime_artifacts(
            "scripts",
            file_path.stem,
            log_level=options.log_level,
        ) as artifacts:
            record = ScriptRunRecord(
                host=host,
                attempts=0,
                success=False,
                error_type=None,
                error_message=None,
                ended_at=started_at,
                duration_seconds=0.0,
                started_at=started_at,
                script_path=str(file_path),
                log_path=str(artifacts.log_path),
                retries_configured=options.retries,
                summary_path=str(artifacts.summary_path),
            )

            try:
                message = f"🚀 Running '{file_path.name}' script..."
                if options.script_args:
                    message += f" with args: {options.script_args}"
                logger.info(message)

                attempts, error = self._invoke_script(file_path, options)
                record.ended_at = datetime.now()
                record.attempts = attempts
                record.duration_seconds = (
                    record.ended_at - record.started_at
                ).total_seconds()

                if error is None:
                    record.success = True
                    logger.success(f"✅ Script '{file_path.name}' executed successfully")
                    callbacks.dispatch_success_callbacks(record)
                else:
                    record.success = False
                    record.error_type = type(error).__name__
                    record.error_message = str(error)
                    logger.error(f"❌ Error running '{file_path.name}' script: {error}")
                    callbacks.dispatch_failure_callbacks(record, error)

                artifacts.summary_path.write_text(
                    dump_json(asdict(record)),
                    encoding="utf-8",
                )
                self.job_summary.add_record(record)
                return record, error
            finally:
                if verbose_handler is not None:
                    logger.remove(verbose_handler)

    def _invoke_script(
        self,
        file_path: Path,
        options: ScriptExecutionOptions,
    ) -> Tuple[int, Optional[Exception]]:
        """🔍 Invoke a script and return the number of attempts and any exception."""
        script_dir = str(file_path.parent)
        if script_dir not in sys.path:
            logger.debug(f"🔍 Adding '{script_dir}' to sys_path...")
            sys.path.insert(0, script_dir)

        original_argv = deepcopy(sys.argv)
        script_args = list(options.script_args)
        attempts = 0
        last_exception: Optional[Exception] = None

        for attempt in range(options.retries + 1):
            attempts = attempt + 1
            try:
                sys.argv = (
                    [str(file_path)] + script_args if script_args else [str(file_path)]
                )
                with TimeCalculator.context(context=file_path.name):
                    run_path(str(file_path), run_name="__main__")
                return attempts, None
            except Exception as exc:  # noqa: BLE001
                last_exception = exc
                if attempt >= options.retries:
                    if isinstance(exc, ImportError):
                        logger.warning(
                            "🔍 If experiencing import errors, try adding root_dir to the"
                            "config file using scriptman config."
                        )
                    return attempts, exc

                delay = self._compute_retry_delay(attempt)
                logger.warning(
                    f"🔄 Retry {attempt + 1} "
                    f"for '{file_path.name}' "
                    f"in {delay:.2f}s — {exc.__class__.__name__}: {exc}"
                )
                sleep(delay)
            finally:
                sys.argv = original_argv

        return attempts, last_exception

    @staticmethod
    def _compute_retry_delay(attempt: int) -> float:
        """🔄 Compute the retry delay based on the attempt number."""
        retry_cfg = config.settings.get("retry", {})
        base_delay = float(retry_cfg.get("base_delay", 1.0))
        min_delay = float(retry_cfg.get("min_delay", 1.0))
        max_delay = float(retry_cfg.get("max_delay", 10.0))
        return float(min(max(base_delay * (2**attempt), min_delay), max_delay))

    @staticmethod
    def _normalize_scripts(candidates: list[Path]) -> list[Path]:
        """🔍 Normalize a list of script paths."""
        normalized: list[Path] = []
        queue = [Path(script) for script in candidates]

        while queue:
            current = queue.pop(0)
            if current.is_dir():
                logger.info(f"📁 Exploring directory '{current}' for scripts...")
                python_files = sorted(current.glob("**/*.py"))
                if not python_files:
                    logger.warning(f"📁 No Python scripts found in directory '{current}'")
                    continue
                logger.info(
                    f"📁 Found {len(python_files)} Python script(s) in '{current}'"
                )
                queue.extend(python_files)
                continue

            if not current.is_file():
                logger.warning(f"⚠️ Skipping '{current}'. Not a valid file.")
                continue

            normalized.append(current.resolve())

        return normalized

    def save_summary(self, file_path: Optional[Path] = None) -> None:
        """
        📊 Save the job summary to a JSON file.

        Args:
            file_path (Optional[Path]): Path to save the summary.
                If None, saves to a default location in the logs directory.
        """
        self.job_summary.end_session()
        if file_path is None:
            logs_dir = Path(config.settings.get("logs_dir", "logs"))
            summary_dir = logs_dir / "scripts" / "_daily"
            summary_dir.mkdir(parents=True, exist_ok=True)
            file_path = summary_dir / f"{datetime.now().strftime('%Y-%m-%d')}.json"

        payload = self.job_summary.as_payload()
        file_path.write_text(dump_json(payload), encoding="utf-8")
        logger.info(f"📊 Script session summary saved to {file_path}")
