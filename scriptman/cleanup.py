import os
import shutil
from datetime import datetime, timedelta
from typing import Optional

from scriptman import Settings
from scriptman.directories import DirectoryHandler
from scriptman.logs import LogHandler, LogLevel


class CleanUpHandler:
    """
    CleanUpHandler manages cleaning tasks for the ScriptManager application.
    """

    def __init__(self) -> None:
        """
        Initialize the CleanUpHandler and perform cleanup tasks.
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

    def remove_old_log_files(self, number_of_days: int = 30) -> None:
        """
        Remove log files older than the specified number of days.

        Args:
            number_of_days (int): The threshold for log file deletion
                (default: 30 days).
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
        Remove the custom driver folder.
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

    def remove_csv_files(self, ignore_filename: Optional[str] = None):
        """
        Remove CSV files from the downloads directory.

        Args:
            ignore_filename (Optional[str]): Filename of csv to ignore when
                deleting csv files (default: None).
        """
        try:
            if self.directory_handler.downloads_dir:
                files = os.listdir(self.directory_handler.downloads_dir)
                for file in files:
                    if file.endswith(".csv"):
                        filename = file.lower()
                        if ignore_filename and ignore_filename in filename:
                            self._log.message(f"Skipped deleting {filename}")
                            continue

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
