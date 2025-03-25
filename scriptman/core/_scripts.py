from asyncio import gather, run, to_thread
from datetime import datetime
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
                logger.warning(f"⚠ Skipping '{script}'. Not a valid file.")
                scripts.remove(script)
                continue

        with TimeCalculator.context("Scriptman"):
            if config.settings.get("concurrent", True) and len(scripts) > 1:
                self.__execute_scripts_concurrently(scripts)
            else:
                self.__execute_scripts_sequentially(scripts)

            if len(scripts) > 0:
                logger.info(
                    f"✅ Finished running "
                    f"{len(scripts)} script{'s' if len(scripts) > 1 else ''}: \n\t- "
                    + "\n\t- ".join([self.__format_result(script) for script in scripts])
                )
            else:
                logger.warning("🔍 No scripts to run")

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
            lock.release()

    def __create_log_file(self, script_file_path: Path) -> int:
        """🔍 Create a log file for a script."""
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file_name = f"{script_file_path.name}_{timestamp}.log"
        log_file_path = Path(config.settings.logs_dir) / log_file_name
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        return logger.add(
            log_file_path,
            rotation="10 MB",
            compression="zip",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
        )

    def execute(self, file_path: Path) -> bool | Exception:
        """
        🏃🏾‍♂️ Runs a script and logs its output.

        Args:
            file_path (Path): The path of the script to run.

        Returns:
            bool: True if the script executed successfully, or the exception otherwise.
        """
        script_dir = str(file_path.parent)
        log_handler = self.__create_log_file(file_path)

        try:
            if script_dir not in sys_path:
                logger.info(f"🔍 Adding '{script_dir}' to sys_path...")
                sys_path.insert(0, script_dir)

            logger.info(f"🚀 Running '{file_path.name}' script...")
            with TimeCalculator.context(context=file_path.name):
                retries = config.settings.get("retries", 0)
                retry(retries)(run_path)(str(file_path), run_name="__main__")
            logger.success(f"✅ Script '{file_path.name}' executed successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Error running '{file_path.name}' script: {e}")
            if config.on_failure_callback is not None:
                logger.info("📞 Calling callback function...")
                config.on_failure_callback(e)
            if isinstance(e, ImportError):
                logger.warning(
                    "🔍 If experiencing import errors, "
                    "try adding root_dir to the config file using scriptman config."
                )
            return e
        finally:
            logger.remove(log_handler)
