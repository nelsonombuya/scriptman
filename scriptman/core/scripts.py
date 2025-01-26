from asyncio import gather, run, to_thread
from pathlib import Path
from re import MULTILINE, sub

from filelock import FileLock, Timeout
from loguru import logger

from scriptman.core.config import config
from scriptman.utils._retry import retry
from scriptman.utils._time_calculator import TimeCalculator


class ScriptsHandler:
    def run_scripts(self, scripts: list[Path]) -> None:
        """
        🏃‍♂️ Executes a list of scripts either concurrently or sequentially based on the
        configuration.

        Args:
            scripts (list[Path]): A list of script paths to be executed.
        """
        for script in scripts:
            if not script.is_file():
                logger.warning(f"⚠ Skipping '{script.name}'. Not a valid file.")
                scripts.remove(script)
                continue

        with TimeCalculator.time_context_manager("🦸‍♂️ Scriptman"):
            if config.env.concurrent and len(scripts) > 1:
                self._execute_scripts_concurrently(scripts)
            else:
                self._execute_scripts_sequentially(scripts)
            logger.info(
                f"✅ Finished running "
                f"{len(scripts)} script{'s' if len(scripts) > 1 else ''}:"
                "\n\t- " + "\n\t- ".join(script.name for script in scripts)
            )

    def _execute_scripts_sequentially(self, scripts: list[Path]) -> None:
        """
        Executes a list of scripts sequentially.

        Args:
            scripts (list[Path]): A list of Path objects representing the scripts to be
                executed.
        """
        for script in scripts:
            self._run_script(script)

    def _execute_scripts_concurrently(self, scripts: list[Path]) -> None:
        """
        Executes a list of scripts concurrently.

        This method runs all the provided scripts asynchronously using asyncio.

        Args:
            scripts (list[Path]): A list of Path objects representing the scripts to be
                executed.
        """

        async def _run_all_scripts_async(scripts: list[Path]) -> None:
            await gather(*(to_thread(self._run_script, script) for script in scripts))

        run(_run_all_scripts_async(scripts))

    def _run_script(self, script: Path):
        """
        🏃‍♂️ Runs a single script with a file lock mechanism.

        Args:
            script (Path): The path of the script to run.
        """
        # FIXME: Log each script to a separate file, without the logs mixing up
        lock_file = script.with_suffix(script.suffix + ".lock")
        lock = FileLock(lock_file, timeout=0)

        try:
            if not config.env.force:
                logger.debug(f"🔐 Acquiring lock for '{script.name}'...")
                with lock:
                    logger.debug(f"🔒 Lock acquired for '{script.name}'.")
                    self._execute_with_logging(script)
            else:
                logger.warning(f"⚠ Force flag set. Skipping lock for '{script.name}'.")
                self._execute_with_logging(script)
        except Timeout:
            logger.error(
                f"Another instance of '{script.name}' is already running. "
                "Run using --force to override."
            )
        finally:
            logger.debug(f"🔓 Releasing lock for '{script.name}'.")

    def _execute_with_logging(self, script: Path):
        """
        ⚙ Executes the script and logs output.

        Args:
            script (Path): The script to execute.
        """
        success, error, details = self.execute(script)

        if not success:
            if config.callback_function is not None:
                config.callback_function(
                    error or Exception("A general error has occurred"),
                    details or {},
                )

    def execute(self, file_path: Path) -> tuple[bool, Exception | None, dict | None]:
        try:
            with open(file_path, "r") as script_file:
                script_content = script_file.read()

            # Replace 'if __name__ == "__main__":' with the module name
            script_content = sub(
                r'^if __name__ == "__main__":',
                f'if __name__ == "{__name__}":',
                script_content,
                flags=MULTILINE,
            )

            logger.info(f"🚀 Running '{file_path.name}' script...")
            retries = getattr(config.env, "retries", 0)
            with TimeCalculator.time_context_manager(file_path.name):
                retry(retries)(exec)(script_content, globals())
            message = f"Script '{file_path.name}' executed successfully"
            logger.success(message)
            return True, None, {"message": message}
        except Exception as e:
            logger.error(f"❌ Error running '{file_path.name}' script: {e}")
            return False, e, {"error": str(e)}
