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

import scriptman._selenium as Selenium
from scriptman._directories import DirectoryHandler
from scriptman._logs import LogHandler, LogLevel
from scriptman._settings import SBI, Settings


class ScriptsHandler:
    """
    Manages the execution and testing of scripts.
    """

    def __init__(
        self,
        scripts_dir: Optional[str] = None,
        upon_failure: Optional[Callable[[str], None]] = None,
    ) -> None:
        """
        Initializes the ScriptsHandler.

        Args:
            scripts_dir (str, optional): The directory where scripts are
                located.
            upon_failure (callable(str, None), optional): A function to call
                upon script execution failure. It should take a string
                argument, where it will receive the stacktrace. It should also
                return None.
        """
        self.upon_failure = upon_failure or (lambda stacktrace: None)
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

        result = ScriptExecutor(log_handler).execute(file, directory, force)

        if not result[0] and isinstance(result[1], str):
            self.upon_failure(result[1])

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

    def execute(
        self,
        file: str,
        directory: str,
        force: bool = False,
    ) -> tuple[bool, str]:
        """
        Execute a Python script.

        Args:
            file (str): The script to execute.
            directory (str): The directory of the script to execute.
            force (bool): Whether to run the file even if there's an existing
                instance. Defaults to False.

        Returns:
            bool, Exception: True if executed successfully, False otherwise
                with the Exception.
        """
        self.exception = None
        self.file = os.path.join(directory, file)
        self.lock_file = f"{file.replace('.', '_')}.lock"
        self.lock_file = os.path.join(directory, self.lock_file)

        try:
            # Replace 'if __name__ == "__main__":' with the module name
            with open(self.file, "r") as script_file:
                script_content = script_file.read()
            script_content = re.sub(
                r'^if __name__ == "__main__":',
                f'if __name__ == "{__name__}":',
                script_content,
                flags=re.MULTILINE,
            )

            # Create a lock file to prevent script from being re-run
            if os.path.exists(self.lock_file) and not force:
                raise FileExistsError
            else:
                open(self.lock_file, "w").close()

            exec(script_content, globals())
            message = f"{self.script_log.title} Script ran successfully"
            message += " after recovery." if self.recovery_mode else "!"
            self.script_log.message(message)
            return True, message
        except FileExistsError as e:
            self.exception = e
            self._handle_script_exceptions(self._locked_script)
            return False, traceback.format_exc()
        except self.selenium_session_exceptions as e:
            self.exception = e
            self._handle_script_exceptions(self._configure_custom_driver)
            return self.execute(file, directory, True)
        except self.selenium_optimization_exceptions as e:
            self.exception = e
            if Settings.selenium_optimizations_mode:
                self._handle_script_exceptions(self._disable_optimizations)
                return self.execute(file, directory, True)
            else:
                self._handle_script_exceptions(self._change_browser)
                return self.execute(file, directory, True)
        except Selenium.InvalidBrowserSelectionError as e:
            self.exception = e
            self._handle_script_exceptions(self._log_selenium_failure)
            return False, traceback.format_exc()
        except Exception as e:
            self.exception = e
            self._handle_script_exceptions(self._log_general_exception)
            return False, traceback.format_exc()
        finally:
            SBI.set_index(0)
            if self._is_not_a_file_lock_exception():
                os.remove(self.lock_file)

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

    def _change_browser(self) -> None:
        """
        Changes selected Selenium Browser to use.
        """
        SBI.set_index(SBI.get_index() + 1)
        self.script_log.message("Re-Running Selenium with another browser.")

    def _log_selenium_failure(self) -> None:
        """
        Logs a failure in a Selenium operation that cannot be recovered.
        """
        self.script_log.message(
            f"{self.script_log.title} failed due to a Web Page Issue.",
            level=LogLevel.ERROR,
            details={
                "error": str(self.exception),
                "stacktrace": traceback.format_exc(),
            },
        )

    def _locked_script(self) -> None:
        """
        Report to the user that the script is currently running and therefore
        locked.
        """
        time_format = "%Y-%m-%d %H:%M:%S"
        lock_creation_time = os.path.getctime(self.lock_file)
        lock_creation_time = time.localtime(lock_creation_time)
        lock_creation_time = time.strftime(time_format, lock_creation_time)

        self.script_log.message(
            "The script is currently running in another instance. "
            " If this is not the case, kindly delete the .lock file:",
            level=LogLevel.WARN,
            details={
                "script_file": self.file,
                "lock_file": self.lock_file,
                "locked_time": lock_creation_time,
            },
        )

    def _log_general_exception(self):
        """
        Report to the user that the script failed due to a general exception.
        """
        self.script_log.message(
            level=LogLevel.ERROR,
            message="The script failed to run and couldn't recover.",
            details={
                "error": str(self.exception),
                "stacktrace": traceback.format_exc(),
            },
        )

    def _is_not_a_file_lock_exception(self):
        """
        Check whether the exception is a FileExistsError and that the lock file
        exists.
        """
        lock_file_exists = os.path.exists(self.lock_file)
        is_FileExistsError = isinstance(self.exception, FileExistsError)
        return (not is_FileExistsError) and lock_file_exists
