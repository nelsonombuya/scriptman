"""
ScriptMan - MaintenanceHandler

This module provides the MaintenanceHandler class, responsible for managing
cleaning and system maintenance tasks for the ScriptManager application.

Usage:
- Import the MaintenanceHandler class from this module.
- Initialize a MaintenanceHandler instance to perform cleanup and maintenance
tasks.

Example:
```python
from scriptman._maintenance import MaintenanceHandler

maintenance_handler = MaintenanceHandler()
# Maintenance tasks are executed upon initialization.
```

Classes:
- `MaintenanceHandler`: Manages cleanup and maintenance tasks for the
ScriptManager application.

For detailed documentation and examples, please refer to the package
documentation.
"""

import os
import shutil
import subprocess
from datetime import datetime, timedelta

from scriptman._directories import DirectoryHandler
from scriptman._logs import LogHandler, LogLevel
from scriptman._settings import Settings


class MaintenanceHandler:
    def __init__(self) -> None:
        """
        Initialize MaintenanceHandler and perform maintenance tasks.

        Maintenance tasks include:
        - Removing "__pycache__" folders.
        - Removing empty and old log files.
        - Moving geckodriver.log to the logs folder.
        - Removing downloaded CSV files from the downloads folder.
        - Running system maintenance scripts (SFC, DISM, Disk Cleanup, Defrag).

        These tasks are executed upon initialization.
        """
        self._log = LogHandler("Maintenance")
        self._directory_handler = DirectoryHandler()

        if Settings.system_maintenance and self._verify_date():
            self._perform_maintenance_tasks()
        else:
            # NOTE: ScriptMan will clean up logs and CSV files on every run.
            self._clean_up()

    def _clean_up(self) -> None:
        """
        Clean up logs and CSV files.
        """
        self._remove_empty_log_files()
        self._remove_old_log_files()
        self._remove_csv_files()

    def _perform_maintenance_tasks(self) -> None:
        """
        Perform maintenance tasks.
        """
        folders_to_clean = Settings.maintenance_folders
        for folder in folders_to_clean:
            self._remove_pycache_folders(folder)

        self._remove_custom_driver_folder()
        self._remove_empty_log_files()
        self._move_geckodriver_log()
        self._remove_old_log_files()
        self._remove_csv_files()
        self._run_system_maintenance()

    def _remove_pycache_folders(self, directory: str) -> None:
        """
        Remove "__pycache__" folders from the given directory.
        """
        for root, dirs, filenames in os.walk(directory):
            if "__pycache__" in dirs:
                dir_path = os.path.join(root, "__pycache__")
                try:
                    shutil.rmtree(dir_path)
                    self._log.message(
                        level=LogLevel.DEBUG,
                        message=f"Deleted {dir_path}",
                        print_to_terminal=Settings.debug_mode,
                    )
                except OSError as error:
                    self._log.message(
                        level=LogLevel.ERROR,
                        details={"Error": error},
                        message=f"Unable to delete {dir_path}.",
                    )

    def _remove_empty_log_files(self) -> None:
        """
        Remove all empty log files in the specified logs directory.
        """
        logs_dir = self._directory_handler.logs_dir
        if os.path.exists(logs_dir):
            for root, _, files in os.walk(logs_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.getsize(file_path) == 0:
                        try:
                            os.remove(file_path)
                            self._log.message(
                                level=LogLevel.DEBUG,
                                print_to_terminal=Settings.debug_mode,
                                message=f"Deleted empty log file: {file_path}",
                            )
                        except OSError as error:
                            self._log.message(
                                level=LogLevel.ERROR,
                                details={"Error": error},
                                message=f"Unable to delete {file_path}.",
                            )

    def _remove_old_log_files(self) -> None:
        """
        Remove log files older than the specified number of days.
        """
        logs_dir = self._directory_handler.logs_dir
        if os.path.exists(logs_dir):
            days_ago = datetime.now() - timedelta(
                days=Settings.clean_up_logs_after_n_days
            )
            for root, _, files in os.walk(logs_dir):
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

    def _remove_custom_driver_folder(self) -> None:
        """
        Remove the custom driver folder if it exists and the setting to keep
        downloaded custom drivers is not enabled.
        """
        selenium_dir = self._directory_handler.selenium_dir
        if (
            os.path.exists(selenium_dir)
            and not Settings.selenium_keep_downloaded_custom_driver
        ):
            try:
                shutil.rmtree(selenium_dir)
                self._log.message(
                    level=LogLevel.DEBUG,
                    print_to_terminal=Settings.debug_mode,
                    message=f"Deleted {selenium_dir}",
                )
            except OSError as error:
                self._log.message(
                    level=LogLevel.ERROR,
                    details={"Error": error},
                    message=f"Unable to delete {selenium_dir}.",
                )

    def _remove_csv_files(self) -> None:
        """
        Remove CSV files from the downloads directory.
        """
        downloads_dir = self._directory_handler.downloads_dir
        try:
            for file in os.listdir(downloads_dir):
                if file.endswith(".csv"):
                    file_path = os.path.join(downloads_dir, file)
                    os.remove(file_path)
                    self._log.message(
                        level=LogLevel.DEBUG,
                        message=f"Removed {file}",
                        print_to_terminal=Settings.debug_mode,
                    )
        except Exception as error:
            self._log.message(
                level=LogLevel.ERROR,
                message=f"An error occurred when deleting CSV files: {error}",
            )

    def _run_system_maintenance(self) -> None:
        """
        Run system maintenance tasks including SFC System Scan & Repair, DISM
        System Scan & Repair, Disk Cleanup, Disk Defragmentation, and
        optionally restart the system upon completion.

        This method performs the following maintenance tasks:
        1. SFC System Scan & Repair: Runs the System File Checker to scan and
            repair corrupted system files.
        2. DISM System Scan & Repair: Runs the Deployment Imaging Service and
            Management Tool to clean up and repair the Windows image.
        3. Disk Cleanup: Initiates the Windows Disk Cleanup utility with a
            specific configuration defined by `Settings.sagerun_code`.
        4. Disk Defragmentation: Runs the disk defragmentation process for the
            current drive.

        If the `Settings.restart_system_after_maintenance` option is enabled,
        the system will be scheduled to restart after a delay of 5 minutes.

        Note:
        - Running system maintenance tasks may require administrative
            privileges.
        - Ensure that the necessary system utilities (e.g., sfc, dism,
            cleanmgr, defrag) are available on the system and accessible in the
            system's PATH.
        """
        try:
            commands = [
                # SFC System Scan & Repair
                ["sfc", "/scannow"],
                # DISM System Scan & Repair
                [
                    "dism",
                    "/cleanup-image",
                    "/online",
                    "/restorehealth",
                ],
                # Disk Cleanup
                [
                    "cleanmgr",
                    f"/sagerun:{Settings.sagerun_code}",
                ],
                # Disc Defragmentation
                ["defrag", "/C"],
            ]
            for command in commands:
                subprocess.run(command)
            if Settings.restart_system_after_maintenance:
                subprocess.run(["shutdown", "/r", "/t", "300"])
        except OSError as error:
            self._log.message(
                level=LogLevel.ERROR,
                details={"Error": error},
                message="An error occurred while running system maintenance.",
            )

    def _move_geckodriver_log(self) -> None:
        """
        Move the geckodriver.log file to the logs directory.
        """
        logs_dir = self._directory_handler.logs_dir
        root_dir = self._directory_handler.root_dir
        geckodriver_log_path = os.path.join(root_dir, "geckodriver.log")

        try:
            if os.path.exists(geckodriver_log_path):
                timestamp = os.path.getctime(geckodriver_log_path)
                timestamp = datetime.fromtimestamp(timestamp)
                timestamp = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
                filename = f"GECKODRIVER - {timestamp}.log"
                destination_path = os.path.join(logs_dir, filename)
                shutil.move(geckodriver_log_path, destination_path)
                self._log.message(
                    level=LogLevel.DEBUG,
                    print_to_terminal=Settings.debug_mode,
                    message=f"Moved geckodriver.log to {destination_path}",
                )
            else:
                self._log.message(
                    level=LogLevel.DEBUG,
                    print_to_terminal=Settings.debug_mode,
                    message=f"geckodriver.log not found in {root_dir}.",
                )
        except OSError as error:
            self._log.message(
                level=LogLevel.ERROR,
                details={"Error": error},
                message=f"Error moving geckodriver.log to {logs_dir}",
            )

    def _verify_date(self) -> bool:
        """
        Verifies if the system maintenance date is today or within the valid
        range.
        """
        current_date = datetime.today()
        max_day = self._get_max_day(current_date.month, current_date.year)
        run_date = (
            Settings.system_maintenance_day
            if Settings.system_maintenance_day <= max_day
            else max_day
        )
        return run_date == current_date.day

    @staticmethod
    def _get_max_day(month: int, year: int) -> int:
        """
        Returns the maximum number of days in the given month and year.
        """
        if month == 2:  # February
            if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
                return 29  # Leap year
            return 28  # Non-leap year
        if month in [4, 6, 9, 11]:  # Months with 30 days
            return 30
        return 31  # All other months
