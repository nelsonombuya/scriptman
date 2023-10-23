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
    """
    MaintenanceHandler manages cleaning and maintenance tasks for the
    ScriptManager application.
    """

    def __init__(self) -> None:
        """
        Initialize MaintenanceHandler and perform cleanup tasks.

        Cleanup tasks include:
        - Removing "__pycache__" folders.
        - Removing old log files.
        - Removing CSV files.
        - Running system maintenance scripts (SFC, DISM, Disk Cleanup, Defrag).

        These tasks are executed upon initialization.
        """
        self._log = LogHandler("Maintenance")
        self._directory_handler = DirectoryHandler()

        if Settings.system_maintenance and self._verify_date():
            self._perform_cleanup()
        else:
            self._log.message("System Maintenance Skipped", LogLevel.DEBUG)

    def _perform_cleanup(self) -> None:
        """
        Perform cleanup tasks.

        Cleanup tasks include:
        - Removing "__pycache__" folders.
        - Removing old log files.
        - Removing CSV files.
        - Running system maintenance scripts (SFC, DISM, Disk Cleanup, Defrag).
        """
        for folder in Settings.maintenance_folders:
            self.remove_pycache_folders(folder)

        self.remove_custom_driver_folder()
        self.remove_empty_log_files()
        self.remove_old_log_files()
        self.remove_csv_files()
        self.run_system_maintenance()

    def remove_pycache_folders(self, directory: str) -> None:
        """
        Remove "__pycache__" folders from the given directory.

        Args:
            directory (str): The directory to search for "__pycache__" folders.
        """
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

    def remove_empty_log_files(self) -> None:
        """
        Remove all empty log files in the specified logs directory.
        """
        if os.path.exists(self._directory_handler.logs_dir):
            for root, dirs, files in os.walk(self._directory_handler.logs_dir):
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

    def remove_old_log_files(self) -> None:
        """
        Remove log files older than the number of days specified in
        Settings.clean_up_logs_after_n_days.
        """
        if os.path.exists(self._directory_handler.logs_dir):
            days_ago = datetime.now() - timedelta(
                days=Settings.clean_up_logs_after_n_days
            )

            for root, dirs, files in os.walk(self._directory_handler.logs_dir):
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
            os.path.exists(self._directory_handler.selenium_dir)
            and not Settings.selenium_keep_downloaded_custom_driver
        ):
            shutil.rmtree(self._directory_handler.selenium_dir)
            self._log.message(
                level=LogLevel.DEBUG,
                print_to_terminal=Settings.debug_mode,
                message=f"Deleted {self._directory_handler.selenium_dir}",
            )

    def remove_csv_files(self):
        """
        Remove CSV files from the downloads directory. Also, ignore csvs with
        filenames contained in Settings.ignore_csv_filename_during_maintenance.
        """
        try:
            files = os.listdir(self._directory_handler.downloads_dir)
            for file in files:
                if file.endswith(".csv"):
                    skip_file = False

                    for n in Settings.ignore_csv_filename_during_maintenance:
                        if n.lower() in file.lower():
                            self._log.message(f"Skipped deleting {file}")
                            skip_file = True
                            break

                    if not skip_file:
                        file_path = os.path.join(
                            self._directory_handler.downloads_dir, file
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
                message=f"An error occurred when deleting csv files: {e}",
            )

    def run_system_maintenance(self):
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
            # SFC System Scan & Repair
            subprocess.run(["sfc", "/scannow"])

            # DISM System Scan & Repair
            subprocess.run(
                [
                    "dism",
                    "/cleanup-image",
                    "/online",
                    "/restorehealth",
                ]
            )

            # Disk Cleanup
            subprocess.run(["cleanmgr", f"/sagerun:{Settings.sagerun_code}"])

            # Disc Defrag
            subprocess.run(["defrag", "/C"])

            # Restart System upon completion (if enabled)
            if Settings.restart_system_after_maintenance:
                subprocess.run(["shutdown", "/r", "/t", "300"])
        except OSError as error:
            self._log.message(
                level=LogLevel.ERROR,
                details={"error": error},
                message="An error occurred while running system maintenance",
            )

    def _verify_date(self) -> bool:
        """
        Verifies if the system maintenance date is today or within the valid
        range.

        This method calculates the maximum day for the target month, accounting
        for leap years, and determines if the system maintenance date is today
        or within the valid range.

        Returns:
            bool: True if the system maintenance date is today or within the
            valid range, False otherwise.
        """
        current_date = datetime.today()

        if current_date.month == 2:  # February
            if current_date.year % 4 == 0 and (
                current_date.year % 100 != 0 or current_date.year % 400 == 0
            ):
                max_day = 29  # Leap year
            else:
                max_day = 28  # Non-leap year
        elif current_date.month in [4, 6, 9, 11]:  # Months with 30 days
            max_day = 30
        else:
            max_day = 31  # All other months

        # Check if the set system maintenance date is within the valid range
        run_date = (
            Settings.system_maintenance_day
            if Settings.system_maintenance_day <= max_day
            else max_day
        )

        return run_date == current_date.day
