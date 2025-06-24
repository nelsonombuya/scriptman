try:
    from abc import ABC, abstractmethod
    from enum import Enum
    from pathlib import Path
    from typing import Generic

    from loguru import logger
    from selenium.webdriver import Chrome as ChromeDriver

    from scriptman.core.config import config
    from scriptman.powers.generics import T
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[selenium]."
    )


class Browsers(Enum):
    """
    🌐 Browser Enums

    This enum contains the different browsers that Scriptman supports.

    Attributes:
        CHROME (str): Google Chrome
    """

    CHROME = "Google Chrome"

    def __str__(self) -> str:
        return self.value


Driver = ChromeDriver


class SeleniumBrowser(ABC, Generic[T]):
    _driver: T
    _local_mode: bool = config.settings.get("selenium_local_mode", True)

    def __init__(self) -> None:
        """
        🚀 Initialize the SeleniumBrowser instance and set the WebDriver instance.

        This method calls the abstract method `_get_driver` to initialize the WebDriver
        instance and assigns it to the `_driver` attribute.
        """
        self.log = logger.bind(handler=self.__class__.__qualname__)
        self._driver = self._get_driver()

    @abstractmethod
    def _get_driver(self) -> T:
        """
        🏎 Get the WebDriver instance associated with the current browser.

        Returns:
            T: The WebDriver instance.
        """
        pass

    @property
    def driver(self) -> T:
        """
        🏎 Get the WebDriver instance associated with the current Selenium browser.

        Returns:
            T: The WebDriver instance (Chrome, Edge, or Firefox) used by the browser.
        """
        return self._driver


def get_browser_default_download_dir() -> Path:
    """
    📁 Get the browser's default download directory for the current operating system.

    Returns:
        Path: The browser's default download directory path.
    """
    system = str(__import__("platform").system()).capitalize()

    if system == "Windows":
        # Windows: The browser typically uses the Downloads folder
        downloads = Path.home() / "Downloads"
        if downloads.exists():
            return downloads
        # Fallback to OneDrive Downloads if it exists
        onedrive_downloads = Path.home() / "OneDrive" / "Downloads"
        if onedrive_downloads.exists():
            return onedrive_downloads
        return downloads

    elif system == "Darwin":  # macOS
        # macOS: The browser uses the Downloads folder
        return Path.home() / "Downloads"

    elif system == "Linux":
        # Linux: The browser uses the Downloads folder
        return Path.home() / "Downloads"

    else:
        # Fallback for unknown systems (Windows, macOS, Linux)
        return Path.home() / "Downloads"
