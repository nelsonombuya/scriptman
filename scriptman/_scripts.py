"""
ScriptMan - ScriptsHandler and ScriptExecutor

This module provides the ScriptsHandler and ScriptExecutor classes for
managing and executing Python scripts.

Usage:
- Import the ScriptsHandler and ScriptExecutor classes from this module.
- Initialize a ScriptsHandler instance to manage script execution and provide
custom recovery actions.
- Use the run_scripts method to execute scripts from a specified directory or
get a list of available scripts.
- Use the run_custom_scripts method to execute custom scripts from their file
paths.

Example:
```python
from scriptman._scripts import ScriptsHandler

# Initialize a ScriptsHandler instance
script_handler = ScriptsHandler()

# Run all scripts in the default scripts directory
script_handler.run_scripts()

# Run specific scripts by providing their filenames
script_handler.run_scripts(scripts=["script1.py", "script2"])

# Execute custom scripts from their file paths
script_handler.run_custom_scripts(
    script_paths=["/path/to/script1.py", "/path/to/script2.py"],
)
```

Classes:
- `ScriptsHandler`: Manages the execution and testing of scripts.
- `ScriptExecutor`: Executes Python scripts and handles exceptions.

Attributes:
- None

Methods (ScriptsHandler):
- `__init__(
        self,
        scripts_dir: Optional[str] = None,
        upon_failure: Optional[Callable] = None
    ) -> None`: Initializes a ScriptsHandler instance.
- `run_scripts(
        self,
        scripts: Optional[List[str]] = None
    ) -> None`: Run specified scripts from the scripts directory.
- `run_custom_scripts(
        self,
        script_paths: List[str]
    ) -> None`: Run specified custom scripts.
- `get_scripts(self) -> List[str]`: Get a list of Python script filenames in
the scripts directory.

Methods (ScriptExecutor):
- `__init__(self, log_handler: LogHandler) -> None`: Initializes a
ScriptExecutor instance.
- `execute(self, file: str, directory: str) -> bool`: Execute a Python script.
"""

import os
import re
import time
import traceback
from typing import Callable, List, Optional

import selenium.common.exceptions as sce

from scriptman._directories import DirectoryHandler
from scriptman._logs import LogHandler, LogLevel
from scriptman._settings import Settings


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

    def run_scripts(
        self,
        scripts: Optional[List[str]] = None,
        force: bool = False,
    ) -> None:
        """
        Run specified scripts from the scripts directory.

        Args:
            scripts (Optional[List[str]]): A list of script filenames to
                execute. If not provided, all '.py' files in the scripts
                directory are executed.
            force (bool): Whether to run the file even if there's an existing
                instance. Defaults to False.
        """
        for file in scripts or self.get_scripts():
            self._execute_script(file, self.scripts_dir, force)

    def run_custom_scripts(
        self,
        script_paths: List[str],
        force: bool = False,
    ) -> None:
        """
        Run specified custom scripts.

        Args:
            script_paths (List[str]): A list of script file paths to execute.
            force (bool): Whether to run the file even if there's an existing
                instance. Defaults to False.
        """
        for file_path in script_paths:
            filename = os.path.basename(file_path)
            directory = os.path.dirname(file_path)
            self._execute_script(filename, directory, force)

    def get_scripts(self) -> List[str]:
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

    def _execute_script(
        self,
        file: str,
        directory: str,
        force: bool = False,
    ) -> None:
        """
        Execute a Python script.

        Args:
            file (str): The script to execute.
            directory (str): The directory of the script to execute.
            force (bool): Whether to run the file even if there's an existing
                instance. Defaults to False.
        """
        filename = os.path.splitext(file)[0]
        extension = os.path.splitext(file)[1]

        if not extension:
            extension = ".py"
            file = filename + extension

        if extension != ".py":
            raise ValueError(f"{file} is not a Python '.py' file.")

        log_handler = LogHandler(filename.title().replace("_", " "))
        log_handler.start()

        if not ScriptExecutor(log_handler).execute(file, directory, force):
            self.upon_failure()

        log_handler.stop()


class ScriptExecutor:
    """
    Executes Python scripts and handles exceptions.
    """

    def __init__(self, log_handler: LogHandler) -> None:
        """
        Initializes the ScriptExecutor.
        """
        self.recovery_mode = False
        self.script_log = log_handler
        self.selenium_session_exceptions = sce.SessionNotCreatedException
        self.selenium_optimization_exceptions = (
            sce.NoSuchElementException,
            sce.WebDriverException,
        )

    def execute(self, file: str, directory: str, force: bool = False) -> bool:
        """
        Execute a Python script.

        Args:
            file (str): The script to execute.
            directory (str): The directory of the script to execute.
            force (bool): Whether to run the file even if there's an existing
                instance. Defaults to False.

        Returns:
            bool: True if executed successfully, False otherwise.
        """
        lock_file = os.path.join(directory, f"{file.replace('.', '_')}.lock")

        try:
            # Replace 'if __name__ == "__main__":' with the module name
            with open(os.path.join(directory, file), "r") as script_file:
                script_content = script_file.read()
            script_content = re.sub(
                r'^if __name__ == "__main__":',
                f'if __name__ == "{__name__}":',
                script_content,
                flags=re.MULTILINE,
            )

            # Create a lock file to prevent script from being re-run
            if os.path.exists(lock_file) and not force:
                raise FileExistsError
            else:
                open(lock_file, "w").close()

            exec(script_content, globals())
            message = f"{self.script_log.title} Script ran successfully"
            message += " after recovery." if self.recovery_mode else "!"
            self.script_log.message(message)
            os.remove(lock_file)
            return True
        except FileExistsError:
            time_format = "%Y-%m-%d %H:%M:%S"
            lock_creation_time = os.path.getctime(lock_file)
            lock_creation_time = time.localtime(lock_creation_time)
            lock_creation_time = time.strftime(time_format, lock_creation_time)

            self.script_log.message(
                "The script is currently running in another instance. "
                " If this is not the case, kindly delete the .lock file:",
                level=LogLevel.WARN,
                details={
                    "lock_file": lock_file,
                    "locked_time": lock_creation_time,
                    "script_file": os.path.join(directory, file),
                },
            )
            return False
        except self.selenium_session_exceptions:
            self._handle_script_exceptions(self._configure_custom_driver)
            os.remove(lock_file)
            return self.execute(file, directory)
        except self.selenium_optimization_exceptions:
            if not Settings.selenium_optimizations_mode:
                raise Exception(
                    f"{self.script_log.title} failed due to a Web Page Issue."
                )  # Prevents recursive loop
            self._handle_script_exceptions(self._disable_optimizations)
            os.remove(lock_file)
            return self.execute(file, directory)
        except Exception as exception:
            stacktrace = traceback.format_exc()
            self.script_log.message(
                level=LogLevel.ERROR,
                message="The script failed to run and couldn't recover.",
                details={"error": str(exception), "stacktrace": stacktrace},
            )
            os.remove(lock_file)
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
        Settings.enable_selenium_custom_driver_mode()

    def _disable_optimizations(self) -> None:
        """
        Disable Selenium optimizations.
        """
        self.script_log.message("Re-Running Selenium Script")
        self.script_log.message("Disabling Selenium Optimizations")
        Settings.disable_selenium_optimizations_mode()
