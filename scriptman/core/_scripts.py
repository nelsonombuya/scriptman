from asyncio import gather, run, to_thread
from datetime import datetime
from pathlib import Path
from re import MULTILINE, sub

from loguru import logger

from scriptman.core._config import config_handler
from scriptman.utils._retry import retry


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

        if config_handler.config.concurrent and len(scripts) > 1:
            self._execute_scripts_concurrently(scripts)
        else:
            self._execute_scripts_sequentially(scripts)

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
        # FIXME: Log each script to a separate file, without the logs mixing up
        log_file = (
            Path(config_handler.config.logs_dir)
            / f"{script.stem}-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        )
        logger.add(log_file, rotation="1 day", retention="30 days")

        try:
            success, error, details = self.execute(script)

            if not success:
                if config_handler.callback_function is not None:
                    config_handler.callback_function(
                        error or Exception("A general error has occurred"),
                        details or {},
                    )
        finally:
            logger.remove()

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
            retries = getattr(config_handler.config, "retries", 0)
            retry(retries)(exec)(script_content, globals())
            message = f"'{file_path.name}' script ran successfully"
            logger.success(message)
            return True, None, {"message": message}
        except Exception as e:
            logger.error(f"Error while running '{file_path.name}': {str(e)}")
            return False, e, {"error": str(e)}
