from asyncio import gather, run, to_thread
from pathlib import Path
from runpy import run_path
from sys import path as sys_path

from filelock import FileLock, Timeout
from loguru import logger

from scriptman.core.config import config
from scriptman.powers.retry import retry
from scriptman.powers.time_calculator import TimeCalculator


class Scripts:
    def __init__(self) -> None:
        """
        ✨ Initializes the Scripts class with an empty dictionary to store
        script execution results.

        The dictionary maps each script path to a boolean indicating whether
        the script executed successfully, or an exception if the script
        execution failed.
        """
        self.__results: dict[Path, bool | Exception] = {}

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

        with TimeCalculator.context("Scriptman"):
            if config.settings.get("concurrent", True) and len(scripts) > 1:
                self.__execute_scripts_concurrently(scripts)
            else:
                self.__execute_scripts_sequentially(scripts)

            logger.info(
                f"✅ Finished running "
                f"{len(scripts)} script{'s' if len(scripts) > 1 else ''}: \n\t- "
                + "\n\t- ".join([self.__format_result(script) for script in scripts])
            )

    def __format_result(self, script: Path) -> str:
        """
        ✍🏾 Formats the result of a single script execution into a string.

        Args:
            script (Path): The path of the script that was executed.

        Returns:
            str: A string that represents the status of the script execution.
        """
        result = self.__results.get(script, Exception("Script not executed"))
        if result is True:
            status = "✅"
        else:
            status = f"❌ -> {type(result).__qualname__}: {str(result)}"
        return f"{script.name} - {status}"

    def __execute_scripts_sequentially(self, scripts: list[Path]) -> None:
        """
        🏃🏾‍♂️🏃🏾‍♂️ Executes a list of scripts sequentially.

        Args:
            scripts (list[Path]): A list of Path objects representing the scripts to be
                executed.

        Returns:
            dict[Path, Any]: A dictionary mapping each script to its execution result.
        """
        for script in scripts:
            self.__lock_and_load_script(script)

    def __execute_scripts_concurrently(self, scripts: list[Path]) -> None:
        """
        🛤 Executes a list of scripts concurrently.

        This method runs all the provided scripts asynchronously using asyncio.

        Args:
            scripts (list[Path]): A list of Path objects representing the scripts to be
                executed.
        """

        async def __run_all_scripts_async(scripts: list[Path]) -> None:
            await gather(*(to_thread(self.__lock_and_load_script, s) for s in scripts))

        run(__run_all_scripts_async(scripts))

    def __lock_and_load_script(self, script: Path) -> None:
        """🔫 Lock and load script for execution."""
        # TODO: Log each script to a separate file, without the logs mixing up
        lock = FileLock(script.with_suffix(script.suffix + ".lock"), timeout=0)

        try:
            if not config.settings.get("force", False):
                logger.debug(f"🔐 Acquiring lock for '{script.name}'...")
                with lock:
                    logger.debug(f"🔒 Lock acquired for '{script.name}'.")
                    self.__results[script] = self.execute(script)
            else:
                logger.warning(f"⚠ Force flag set. Skipping lock for '{script.name}'.")
                self.__results[script] = self.execute(script)
        except Timeout as e:
            logger.error(
                f"Another instance of '{script.name}' is already running. "
                "Run using --force to override."
            )
            self.__results[script] = e
        finally:
            logger.debug(f"🔓 Releasing lock for '{script.name}'.")

    def execute(self, file_path: Path) -> bool | Exception:
        """
        🏃🏾‍♂️ Runs a script and logs its output.

        Args:
            file_path (Path): The path of the script to run.

        Returns:
            bool: True if the script executed successfully, or the exception otherwise.
        """
        try:
            script_dir = str(file_path.parent)
            if script_dir not in sys_path:
                sys_path.insert(0, script_dir)

            logger.info(f"🚀 Running '{file_path.name}' script...")
            with TimeCalculator.context(context=file_path.name):
                retries = config.settings.get("retries", 0)
                retry(retries)(run_path)(str(file_path), run_name="__main__")
            logger.success(f"Script '{file_path.name}' executed successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Error running '{file_path.name}' script: {e}")
            if config.on_failure_callback is not None:
                logger.info("📞 Calling callback function...")
                config.on_failure_callback(e)
            return e
