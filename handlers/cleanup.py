import os
import shutil
from datetime import datetime, timedelta

from .directories import DirectoryHandler
from .logs import LogHandler, LogLevel
from .settings import settings


class CleanUpHandler:
    """
    CleanUpHandler manages cleaning tasks for the ScriptManager application.
    """

    def __init__(self) -> None:
        self._log = LogHandler("CleanUp Manager")
        self._directories = DirectoryHandler().directories

        self._remove_pycache_folders(self._directories["scriptman"])
        self._remove_pycache_folders(self._directories["root"])
        self._remove_custom_driver_folder()
        self._remove_old_log_files()
        self._remove_csv_files()

    def _remove_pycache_folders(self, directory: str) -> None:
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
                            print_to_terminal=settings.debug_mode,
                        )
                    except OSError as error:
                        self._log.message(
                            level=LogLevel.ERROR,
                            details={"Error": error},
                            message=f"Unable to delete {path}.",
                        )

    def _remove_old_log_files(self, number_of_days: int = 30) -> None:
        """
        Remove log files older than the specified number of days.
        Args:
            number_of_days (int): The threshold for log file deletion
                (default: 30 days).
        """
        logs_directory = os.path.join(self._directories["root"], "logs")

        if logs_directory and os.path.exists(logs_directory):
            days_ago = datetime.now() - timedelta(days=number_of_days)

            for root, dirs, files in os.walk(logs_directory):
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
                                print_to_terminal=settings.debug_mode,
                                message=f"Deleted log file: {file_path}",
                            )
                        except OSError as error:
                            self._log.message(
                                level=LogLevel.ERROR,
                                details={"Error": error},
                                message=f"Unable to delete {file_path}.",
                            )

    def _remove_custom_driver_folder(self) -> None:
        """
        Remove the custom driver folder.
        """
        downloads_dir = os.path.join(self._directories["root"], "downloads")
        custom_driver_directory = os.path.join(downloads_dir, "selenium")

        if os.path.exists(custom_driver_directory):
            shutil.rmtree(custom_driver_directory)
            self._log.message(
                level=LogLevel.DEBUG,
                print_to_terminal=settings.debug_mode,
                message=f"Deleted {custom_driver_directory}",
            )

    def _remove_csv_files(self, remove_tickets_csv: bool = True):
        """
        Remove CSV files from the downloads directory.
        Args:
            remove_tickets_csv (bool): Whether to remove CSV files related to
                tickets (default: True).
        """
        try:
            root_dir = self._directories["root"]
            downloads_dir = os.path.join(root_dir, "downloads")
            if downloads_dir:
                files = os.listdir(downloads_dir)
                for file in files:
                    if file.endswith(".csv"):
                        filename = file.lower()
                        if not remove_tickets_csv and "tickets" in filename:
                            continue

                        file_path = os.path.join(downloads_dir, file)
                        os.remove(file_path)
                        self._log.message(
                            level=LogLevel.DEBUG,
                            message=f"Removed {file}",
                            print_to_terminal=settings.debug_mode,
                        )
        except Exception as e:
            self._log.message(
                level=LogLevel.ERROR,
                message=f"An error occurred: {e}",
            )
