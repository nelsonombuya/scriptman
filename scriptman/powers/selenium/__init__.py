try:
    from abc import ABC
    from fnmatch import fnmatch
    from pathlib import Path
    from random import uniform
    from shutil import move
    from time import sleep
    from typing import Any, Literal, Optional

    from loguru import logger
    from selenium.webdriver import ActionChains
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    from scriptman.core.config import config
    from scriptman.powers.selenium._chrome import Chrome
    from scriptman.powers.selenium._firefox import Firefox
    from scriptman.powers.selenium._utils import (
        Browsers,
        Driver,
        SeleniumBrowser,
        get_browser_default_download_dir,
    )
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[selenium]."
    )

# Define BrowserMap after all imports to avoid circular imports
BrowserMap: dict[Browsers, type[SeleniumBrowser[Any]]] = {
    Browsers.CHROME: Chrome,
    Browsers.FIREFOX: Firefox,
}


class SeleniumInstance(ABC):
    def __init__(self, browser: Browsers = Browsers.CHROME) -> None:
        """
        🚀 Initialize SeleniumInstance with the given browser.

        Args:
            browser (Browsers, optional): The browser to use. Defaults to Browsers.CHROME.
        """
        self._downloaded_files: set[Path] = set()
        self._log = logger.bind(name=self.__class__.__name__)
        self._browser: SeleniumBrowser[Driver] = BrowserMap.get(browser, Chrome)()

    @property
    def driver(self) -> Driver:
        """
        🏎 Get the WebDriver instance associated with the current browser.

        Returns:
            Driver: The WebDriver instance (Chrome, Edge, or Firefox) used by the browser.
        """
        return self._browser.driver

    def interact_with_element(
        self,
        xpath: str,
        timeout: int = 30,
        keys: Optional[str] = None,
        rest: float = uniform(0.25, 0.50),
        element_name: Optional[str] = None,
        mode: Literal[
            "click",
            "js_click",
            "send_keys",
            "send_return",
            "deny_cookies",
            "accept_cookies",
            "wait_till_invisible",
        ] = "click",
    ) -> bool:
        """
        👉🏾 Interact with a web element on the page.

        Args:
            xpath (str): The XPath expression to locate the web element.
            timeout (int, optional): The maximum time (in seconds) to wait for
                the element to become clickable or invisible. Defaults to 30.
            keys (str, optional): The text to send to the element when using
                SEND_KEYS mode. Ignored if mode is not SEND_KEYS.
            rest (float, optional): The time (in seconds) to rest after the
                interaction. Defaults to a random time between 0.25s and 0.50s.
            element_name (str, optional): The name of the element to interact with.
                Defaults to None.
            mode (Literal, optional): The interaction mode. Defaults to "click".

        Returns:
            bool: True if the interaction was successful, False otherwise.
        """
        self._log.debug(
            f"Interacting with element: {xpath} (mode: {mode}) "
            f"Element name: {element_name} "
            f"Timeout: {timeout} "
            f"Rest: {rest}."
        )

        sleep(rest)  # Rest before each interaction to offset the bot detection
        if mode in ["deny_cookies", "accept_cookies"]:  # Deny or accept cookies
            xpath = xpath or (
                '//*[@id="tarteaucitronAllDenied2"]'
                if mode == "deny_cookies"
                else '//*[@id="tarteaucitronAllAllowed2"]'
            )
            mode = "js_click"

        wait = WebDriverWait(self.driver, timeout)
        if mode == "wait_till_invisible":  # Wait for the element to become invisible
            wait.until(EC.invisibility_of_element_located((By.XPATH, xpath)))
            return True

        element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        ActionChains(self.driver).move_to_element(element).perform()  # type: ignore

        if mode == "click":  # Click on the web element
            element.click()
            return True

        if mode == "js_click":  # Perform a JavaScript click on the web element
            self.driver.execute_script("arguments[0].click();", element)  # type: ignore
            return True

        if mode == "send_keys":  # Send keys (text input) to the web element
            if not keys:
                raise ValueError("Keys must be provided for SEND_KEYS mode")
            element.send_keys(keys)
            return True

        if mode == "send_return":  # Send return key to the web element
            element.send_keys(Keys.RETURN)
            return True

        raise ValueError(f"Invalid mode: {mode}")

    def wait_for_downloads_to_finish(
        self,
        search_pattern: Optional[str] = None,
        timeout: int = 300,
        case_sensitive: bool = True,
        mark_for_deletion: bool = True,
    ) -> Path:
        """
        ⌚ Wait for all downloads to finish before continuing.

        Monitors the browser's default download directory and moves files to the
        configured downloads directory after detection.

        Args:
            search_pattern (Optional[str]): A wildcard pattern to search for specific
                files. Supports patterns like "*statement*.csv", "report_*.pdf", etc.
                If None, waits for any new file to be downloaded.
                Defaults to None.
            timeout (int, optional): The maximum time (in seconds) to wait for the
                downloads to finish. Defaults to 300.
            case_sensitive (bool, optional): Whether pattern matching should be case
                sensitive. If False, patterns will match files regardless of case.
                Defaults to True.
            mark_for_deletion (bool, optional): Whether to mark the downloaded file
                for automatic deletion when the SeleniumInstance is garbage collected.
                Defaults to True.

        Returns:
            Path: The path of the recently downloaded file in the configured directory.
        """
        download_extensions = (
            ".crdownload",  # Chrome
            ".part",  # Firefox
            ".tmp",  # Chromium/Other
        )

        # Monitor the browser's default download directory
        browser_download_dir = get_browser_default_download_dir()
        configured_download_dir = Path(config.settings.downloads_dir)

        # Check if directories are the same
        if browser_download_dir.resolve() == configured_download_dir.resolve():
            self._log.debug(
                f"Browser and configured download directories are the same: "
                f"{browser_download_dir}"
            )
            self._log.debug("No file moving will be performed")
        else:
            self._log.debug(
                f"Checking {self._browser} downloads in: {browser_download_dir}"
            )
            self._log.debug(f"Files will be moved to: {configured_download_dir}")

        files = list(browser_download_dir.iterdir())

        if not search_pattern:
            # Wait for any new file
            self._wait_for_new_file(
                timeout=timeout,
                initial_files=files,
                download_extensions=download_extensions,
                browser_download_dir=browser_download_dir,
            )
            downloaded_file = self._get_latest_new_file(
                initial_files=files,
                download_extensions=download_extensions,
                browser_download_dir=browser_download_dir,
            )
        else:
            # Wait for file matching pattern
            self._wait_for_matching_file(
                browser_download_dir=browser_download_dir,
                download_extensions=download_extensions,
                search_pattern=search_pattern,
                case_sensitive=case_sensitive,
                timeout=timeout,
            )
            downloaded_file = self._get_latest_matching_file(
                browser_download_dir=browser_download_dir,
                download_extensions=download_extensions,
                search_pattern=search_pattern,
                case_sensitive=case_sensitive,
            )

        # Move file to configured directory and handle deletion tracking
        if config.settings.get("selenium_auto_move_downloads", False):
            final_path = self._move_file_to_configured_dir(
                source_file=downloaded_file,
                target_dir=configured_download_dir,
            )
        else:
            final_path = downloaded_file
            self._log.debug(
                f"Auto-move is currently disabled, keeping file in browser directory: "
                f"{final_path}"
            )

        if mark_for_deletion:
            self._downloaded_files.add(final_path)

        return final_path

    def _wait_for_new_file(
        self,
        browser_download_dir: Path,
        initial_files: list[Path],
        download_extensions: tuple[str, ...],
        timeout: int,
    ) -> None:
        """Wait for any new file to be added to the download directory."""

        def is_new_file_added(driver: Driver) -> bool:
            current_files = list(browser_download_dir.iterdir())
            new_files = [
                file
                for file in current_files
                if file not in initial_files and file.suffix not in download_extensions
            ]
            return len(new_files) > 0

        WebDriverWait(self.driver, timeout, 1).until(is_new_file_added)

    def _wait_for_matching_file(
        self,
        browser_download_dir: Path,
        search_pattern: str,
        case_sensitive: bool,
        download_extensions: tuple[str, ...],
        timeout: int,
    ) -> None:
        """Wait for a file matching the pattern to be added to the download directory."""

        def does_file_exist(driver: Driver) -> bool:
            current_files = list(browser_download_dir.iterdir())
            matching_files = self._get_matching_files(
                files=current_files,
                search_pattern=search_pattern,
                case_sensitive=case_sensitive,
                download_extensions=download_extensions,
            )
            return len(matching_files) > 0

        WebDriverWait(self.driver, timeout, 1).until(does_file_exist)

    def _get_latest_new_file(
        self,
        browser_download_dir: Path,
        initial_files: list[Path],
        download_extensions: tuple[str, ...],
    ) -> Path:
        """Get the most recently downloaded new file."""
        current_files = list(browser_download_dir.iterdir())
        new_files = [
            file
            for file in current_files
            if file not in initial_files and file.suffix not in download_extensions
        ]
        return max(new_files, key=lambda x: x.stat().st_mtime)

    def _get_latest_matching_file(
        self,
        browser_download_dir: Path,
        search_pattern: str,
        case_sensitive: bool,
        download_extensions: tuple[str, ...],
    ) -> Path:
        """Get the most recently downloaded file matching the pattern."""
        current_files = list(browser_download_dir.iterdir())
        matching_files = self._get_matching_files(
            files=current_files,
            search_pattern=search_pattern,
            case_sensitive=case_sensitive,
            download_extensions=download_extensions,
        )
        return max(matching_files, key=lambda x: x.stat().st_mtime)

    def _get_matching_files(
        self,
        files: list[Path],
        search_pattern: str,
        case_sensitive: bool,
        download_extensions: tuple[str, ...],
    ) -> list[Path]:
        """Get files matching the pattern, excluding download extensions."""
        if case_sensitive:
            return [
                file
                for file in files
                if file.suffix not in download_extensions
                and fnmatch(file.name, search_pattern)
            ]
        else:
            return [
                file
                for file in files
                if file.suffix not in download_extensions
                and fnmatch(file.name.lower(), search_pattern.lower())
            ]

    def _move_file_to_configured_dir(self, source_file: Path, target_dir: Path) -> Path:
        """
        📁 Move a downloaded file from the browser's default directory to the configured
        directory.

        Args:
            source_file (Path): The source file path in the browser's default directory.
            target_dir (Path): The target directory from configuration.

        Returns:
            Path: The final path of the moved file.
        """
        try:
            # Check if source and target directories are the same
            if source_file.parent.resolve() == target_dir.resolve():
                self._log.debug(
                    f"Source and target directories are the same: {target_dir}"
                )

                # Check if a file with the same name already exists
                target_path = target_dir / source_file.name
                if target_path.exists() and target_path != source_file:
                    # Handle filename conflicts by renaming
                    counter = 1
                    while target_path.exists():
                        stem = source_file.stem
                        suffix = source_file.suffix
                        target_path = target_dir / f"{stem}_{counter}{suffix}"
                        counter += 1

                    source_file.rename(target_path)
                    self._log.info(
                        f"Renamed downloaded file due to conflict: "
                        f"{source_file.name} -> {target_path.name}"
                    )
                    return target_path
                else:
                    self._log.debug(f"File already in correct location: {source_file}")
                    return source_file

            # Different directories - perform the move
            # Ensure target directory exists
            target_dir.mkdir(parents=True, exist_ok=True)

            # Create target path
            target_path = target_dir / source_file.name

            # Handle filename conflicts
            counter = 1
            original_target = target_path
            while target_path.exists():
                stem = original_target.stem
                suffix = original_target.suffix
                target_path = target_dir / f"{stem}_{counter}{suffix}"
                counter += 1

            # Move the file
            move(str(source_file), str(target_path))
            self._log.info(f"Moved downloaded file: {source_file.name} -> {target_path}")

            return target_path

        except Exception as e:
            self._log.error(f"Failed to move file {source_file} to {target_dir}: {e}")
            # Return original path if move fails
            return source_file

    def __del__(self) -> None:
        """
        🧹 Close the WebDriver instance when the InteractionHandler instance is deleted.
        """
        name = self.__class__.__name__
        try:
            self._log.info(f"Shutting down SeleniumInstance {name}")
            self.driver.quit()
        except Exception as e:
            self._log.error(f"Failed to close SeleniumInstance {name} : {e}")

        if self._downloaded_files:
            for file in self._downloaded_files:
                try:
                    file.unlink()
                except Exception as e:
                    self._log.warning(f"Failed to remove downloaded file {file}: {e}")


__all__: list[str] = [
    "Chrome",
    "Firefox",
    "Driver",
    "Browsers",
    "BrowserMap",
    "SeleniumBrowser",
    "SeleniumInstance",
    "get_browser_default_download_dir",
]
