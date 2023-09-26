"""
ScriptMan - CleanUpHandler

This module provides the CleanUpHandler class, responsible for managing
cleaning tasks for the ScriptManager application.

Usage:
- Import the CleanUpHandler class from this module.
- Initialize a CleanUpHandler instance to perform cleanup tasks.

Example:
```python
from scriptman._cleanup import CleanUpHandler

cleanup_handler = CleanUpHandler()
# Cleanup tasks are executed upon initialization.
```

Classes:
- `CleanUpHandler`: Manages cleanup tasks for the ScriptManager application.

For detailed documentation and examples, please refer to the package
documentation.
"""

import os
import shutil
from datetime import datetime, timedelta

from scriptman._directories import DirectoryHandler
from scriptman._logs import LogHandler, LogLevel
from scriptman._settings import Settings


class CleanUpHandler:
    """
    CleanUpHandler manages cleaning tasks for the ScriptManager application.

    Attributes:
        directory_handler (DirectoryHandler): An instance of DirectoryHandler
        for managing directories.
    """

    def __init__(self) -> None:
        """
        Initialize CleanUpHandler and perform cleanup tasks.

        Cleanup tasks include:
        - Removing "__pycache__" folders.
        - Removing old log files.
        - Removing CSV files.

        These tasks are executed upon initialization.
        """
        self._log = LogHandler("CleanUp Handler")
        self.directory_handler = DirectoryHandler()
        self.remove_custom_driver_folder()
        self.remove_old_log_files()
        self.remove_csv_files()

        for folder in Settings.clean_up_folders:
            self.remove_pycache_folders(folder)

    def remove_pycache_folders(self, directory: str) -> None:
        """
        Remove "__pycache__" folders from the given directory.

        Args:
            directory (str): The directory to search for "__pycache__" folders.
        """
        if directory:
            for dirpath, dirnames, filenames in os.walk(directory):
                if "__pycache__" in dirnames:
                    dirnames.remove("__pycache__")
                    path = os.path.join(dirpath, "__pycache__")
                    try:
                        shutil.rmtree(path)
                        self._log.message(
                            level=LogLevel.DEBUG,
                            message=f"Deleted {path}",
                            print_to_terminal=Settings.debug_mode,
                        )
                    except OSError as error:
                        self._log.message(
                            level=LogLevel.ERROR,
                            details={"Error": error},
                            message=f"Unable to delete {path}.",
                        )

    def remove_old_log_files(
        self, number_of_days: int = Settings.clean_up_logs_after_n_days
    ) -> None:
        """
        Remove log files older than the specified number of days.

        Args:
            number_of_days (int): The threshold for log file deletion
                (default set in SettingsHandler).
        """
        if os.path.exists(self.directory_handler.logs_dir):
            days_ago = datetime.now() - timedelta(days=number_of_days)

            for root, dirs, files in os.walk(self.directory_handler.logs_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    file_creation_time = datetime.fromtimestamp(
                        os.path.getctime(file_path)
                    )

                    if file_creation_time < days_ago:
                        try:
                            os.remove(file_path)
                            self._log.message(
                                level=LogLevel.DEBUG,
                                print_to_terminal=Settings.debug_mode,
                                message=f"Deleted log file: {file_path}",
                            )
                        except OSError as error:
                            self._log.message(
                                level=LogLevel.ERROR,
                                details={"Error": error},
                                message=f"Unable to delete {file_path}.",
                            )

    def remove_custom_driver_folder(self) -> None:
        """
        Remove the custom driver folder if it exists and the setting to keep
        downloaded custom drivers is not enabled.
        """
        if (
            os.path.exists(self.directory_handler.selenium_dir)
            and not Settings.selenium_keep_downloaded_custom_driver
        ):
            shutil.rmtree(self.directory_handler.selenium_dir)
            self._log.message(
                level=LogLevel.DEBUG,
                print_to_terminal=Settings.debug_mode,
                message=f"Deleted {self.directory_handler.selenium_dir}",
            )

    def remove_csv_files(self):
        """
        Remove CSV files from the downloads directory. Also ignores csvs with
        filenames contained in Settings.ignore_csv_filename_during_cleanup:
        """
        try:
            if self.directory_handler.downloads_dir:
                files = os.listdir(self.directory_handler.downloads_dir)
                for file in files:
                    if file.endswith(".csv"):
                        skip_file = False
                        for n in Settings.ignore_csv_filename_during_cleanup:
                            if file.lower() in n.lower():
                                self._log.message(f"Skipped deleting {file}")
                                skip_file = True
                                break

                        if not skip_file:
                            file_path = os.path.join(
                                self.directory_handler.downloads_dir, file
                            )
                            os.remove(file_path)
                            self._log.message(
                                level=LogLevel.DEBUG,
                                message=f"Removed {file}",
                                print_to_terminal=Settings.debug_mode,
                            )
        except Exception as e:
            self._log.message(
                level=LogLevel.ERROR,
                message=f"An error occurred: {e}",
            )
