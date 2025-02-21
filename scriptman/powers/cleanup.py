import shutil
from datetime import datetime
from os import getcwd
from pathlib import Path
from typing import Union

from loguru import logger

from scriptman.core.config import config


class CleanUp:
    def cleanup(
        self,
        wd: Union[Path, str] = getcwd(),
        delete_empty_files: bool = True,
        delete_files_older_than: int = 30,
        file_globs_to_delete: list[str] = ["*.log", "*.csv"],
        folder_glob_to_delete: list[str] = ["__pycache__", "pytest_cache"],
    ) -> None:
        """
        🧹 Clean up the environment by removing empty files and cache folders.

        Args:
            wd (Union[Path, str]): The current working directory.
            delete_empty_files (bool): Flag to delete empty files.
            delete_files_older_than (int): Days after which the file globs are deleted.
            file_globs_to_delete (List[str]): List of file globs to delete.
            folder_glob_to_delete (List[str]): List of folder globs to delete.
        """
        wd = Path(wd)
        now = datetime.now()
        file_cleanup_errors = []
        folder_cleanup_errors = []

        # Clean files with specified globs
        for file_glob in file_globs_to_delete:
            for file_path in wd.rglob(file_glob):
                try:
                    # Remove empty files if enabled
                    if delete_empty_files and file_path.stat().st_size == 0:
                        logger.debug(f"Removing empty file: {file_path}")
                        self.safe_remove_file(file_path)
                        continue

                    # Remove files older than specified days
                    file_date = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if (now - file_date).days > delete_files_older_than:
                        logger.debug(
                            f"Removing file older than {delete_files_older_than} days: "
                            f"{file_path}"
                        )
                        self.safe_remove_file(file_path)

                except Exception as e:
                    file_cleanup_errors.append(f"Error processing {file_path}: {e}")

        # Remove specified cache folders
        for folder_glob in folder_glob_to_delete:
            for path in wd.rglob(folder_glob):
                if path.is_dir():
                    if not self.safe_remove_directory(path):
                        folder_cleanup_errors.append(str(path))

        # Log errors if any occurred
        if file_cleanup_errors or folder_cleanup_errors:
            error_message = "Cleanup encountered issues:\n"
            if file_cleanup_errors:
                error_message += (
                    "File errors:\n\t" + "\n\t".join(file_cleanup_errors) + "\n"
                )
            if folder_cleanup_errors:
                error_message += "Cache folder errors:\n\t" + "\n\t".join(
                    folder_cleanup_errors
                )

            logger.warning(error_message)

        # Extras
        self.selenium_cleanup()  # Clean up downloaded Selenium files
        self.diskcache_cleanup()  # Clean up diskcache cache
        self.mypy_cleanup()  # Clean up mypy cache

    def safe_remove_file(self, file_path: Union[Path, str]) -> bool:
        """Safely remove a file with error handling."""
        try:
            Path(file_path).unlink()
            return True
        except PermissionError:
            logger.warning(f"Permission denied removing file: {file_path}")
        except OSError as e:
            logger.error(f"Error removing file {file_path}: {e}")
        return False

    def safe_remove_directory(self, dir_path: Union[Path, str]) -> bool:
        """Safely remove a directory with error handling."""
        try:
            shutil.rmtree(dir_path)
            return True
        except PermissionError:
            logger.warning(f"Permission denied removing directory: {dir_path}")
        except OSError as e:
            logger.error(f"Error removing directory {dir_path}: {e}")
        return False

    def diskcache_cleanup(self) -> None:
        """🧹 Clean up diskcache files."""
        try:
            from scriptman.powers.cache._diskcache import DiskCacheBackend

            DiskCacheBackend.clean_cache_dir()
        except ImportError as e:
            logger.warning(f"Skipped cleaning up diskcache files: {e}")

    def selenium_cleanup(self) -> None:
        """🧹 Clean up Selenium downloads and cache folders."""
        try:
            from scriptman.powers.selenium._chrome import ChromeDownloader

            ChromeDownloader.cleanup_chrome_downloads()
        except ImportError as e:
            logger.warning(f"Skipped cleaning up Selenium files: {e}")

    def mypy_cleanup(self) -> None:
        """🧹 Clean up mypy cache files."""
        config.cwd.joinpath(".mypy_cache").unlink(missing_ok=True)
