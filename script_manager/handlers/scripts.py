import os
from typing import Callable, List, Optional

import selenium.common.exceptions as sce

from script_manager.handlers.directories import DirectoryHandler
from script_manager.handlers.logs import LogHandler, LogLevel
from script_manager.handlers.settings import settings


class ScriptsHandler:
    """
    Manages the execution and testing of scripts.
    """

    def __init__(
        self,
        scripts_dir: Optional[str] = None,
        upon_failure: Optional[Callable] = None,
    ) -> None:
        """
        Initializes the ScriptsHandler.

        By default, it sets the scripts directory using DirectoryHandler.

        Args:
            scripts_dir (str, optional): The directory where scripts are
                located.
            upon_failure (callable, optional): A function to call upon script
                execution failure.
        """
        self.upon_failure = upon_failure or (lambda: None)
        self.scripts_dir = scripts_dir or DirectoryHandler().scripts_dir

    def run_scripts(self, scripts: Optional[List[str]] = None) -> None:
        """
        Run specified scripts from the scripts directory.

        Args:
            scripts (Optional[List[str]]): A list of script filenames to
                execute. If not provided, all '.py' files in the scripts
                directory are executed.
        """
        settings.enable_logging()
        settings.disable_debugging()

        for file in scripts or self.get_python_scripts():
            self._execute_script(file, self.scripts_dir)

    def test_scripts(self, scripts: Optional[List[str]] = None) -> None:
        """
        Test scripts in the scripts directory with logging disabled and
        debugging enabled.

        Args:
            scripts (Optional[List[str]]): A list of script filenames to
                execute. If not provided, all '.py' files in the scripts
                directory are executed.
        """
        settings.disable_logging()
        settings.enable_debugging()

        for file in scripts or self.get_python_scripts():
            self._execute_script(file, self.scripts_dir)

    def run_custom_scripts(self, script_paths: List[str]) -> None:
        """
        Run specified custom scripts.

        Args:
            script_paths (List[str]): A list of script file paths to execute.
        """
        settings.enable_logging()
        settings.disable_debugging()

        for file_path in script_paths:
            filename = os.path.basename(file_path)
            directory = os.path.dirname(file_path)
            self._execute_script(filename, directory)

    def test_custom_scripts(self, script_paths: List[str]) -> None:
        """
        Test custom scripts with logging disabled and debugging enabled.

        Args:
            script_paths (List[str]): A list of script file paths to execute.
        """
        settings.disable_logging()
        settings.enable_debugging()

        for file_path in script_paths:
            filename = os.path.basename(file_path)
            directory = os.path.dirname(file_path)
            self._execute_script(filename, directory)

    def get_python_scripts(self) -> List[str]:
        """
        Get a list of Python script filenames in the scripts directory.

        Returns:
            List[str]: A list of '.py' script filenames.
        """
        return [
            filename
            for filename in os.listdir(self.scripts_dir)
            if filename.endswith(".py")
        ]

    def _execute_script(self, filename: str, directory: str) -> None:
        """
        Execute a Python script.

        Args:
            filename (str): The filename of the script to execute.
            directory (str): The directory of the script to execute.
        """
        if not ScriptExecutor().execute(filename, directory):
            self.upon_failure()


class ScriptExecutor:
    """
    Executes Python scripts and handles exceptions.
    """

    def __init__(self) -> None:
        """
        Initializes the ScriptExecutor.
        """
        self.recovery_mode = False
        self.selenium_session_exceptions = sce.SessionNotCreatedException
        self.selenium_optimization_exceptions = (
            sce.NoSuchElementException,
            sce.WebDriverException,
        )

    def execute(self, filename: str, directory: str) -> bool:
        """
        Execute a Python script.

        Args:
            filename (str): The filename of the script to execute.
            directory (str): The directory of the script to execute.

        Returns:
            bool: True if executed successfully, False otherwise.

        Raises:
            ValueError: If the provided file is not a python '.py' file.
        """
        filename = os.path.splitext(filename)[0]
        extension = os.path.splitext(filename)[1]

        if extension != ".py":
            raise ValueError(f"{filename} is not a Python '.py' file.")

        script_name = filename.title().replace("_", " ")
        script_path = os.path.join(directory, f"{filename}.py")

        self.script_log = LogHandler(script_name)

        try:
            self.script_log.start()
            exec(open(script_path).read(), globals())
            message = f"{script_name} Script ran successfully"
            message += " after recovery." if self.recovery_mode else "."
            self.script_log.message(message)
            return True
        except self.selenium_session_exceptions:
            self._handle_script_exceptions(self._configure_custom_driver)
            return self.execute(filename, directory)
        except self.selenium_optimization_exceptions:
            if settings.selenium_optimizations_mode:
                raise Exception(
                    f"{script_name} failed due to a Selenium Issue."
                )  # Prevents recursive loop
            self._handle_script_exceptions(self._disable_optimizations)
            return self.execute(filename, directory)
        except Exception as exception:
            self.script_log.message(
                level=LogLevel.ERROR,
                details={"error": str(exception)},
                message="The script failed to run and couldn't recover.",
            )
            return False

    def _handle_script_exceptions(self, recovery_function: Callable) -> None:
        """
        Handle script execution exceptions.

        Args:
            recovery_function (callable): Function to run for recovery.
        """
        self.script_log.message("Error occurred while running script.")
        self.script_log.message("Trying to recover...")
        self.recovery_mode = True
        recovery_function()

    def _configure_custom_driver(self) -> None:
        """
        Configure Selenium Custom Driver session.
        """
        self.script_log.message("Error starting Chrome Driver Session")
        self.script_log.message("Downloading Custom Chrome App & Driver")
        settings.enable_selenium_custom_driver_mode()

    def _disable_optimizations(self) -> None:
        """
        Disable Selenium optimizations.
        """
        self.script_log.message("Re-Running Selenium Script")
        self.script_log.message("Disabling Selenium Optimizations")
        settings.disable_selenium_optimizations_mode()
