"""
ScriptMan - Firefox Selenium WebDriver Management

This module provides the `Firefox` class for managing Firefox Selenium
WebDriver instances.

Usage:
- Import the `Firefox` class from this module.
- Initialize a `Firefox` instance using `Firefox()`.
- Use the initialized `Firefox` instance to interact with Firefox WebDriver.

Example:
```python
from scriptman._selenium_firefox import Firefox

firefox = Firefox()
# Your Firefox WebDriver instance is ready to use.
```

Classes:
- `Firefox`: Manages the creation and configuration of Firefox Selenium
    WebDriver instances.

Attributes:
- `driver (webdriver.Firefox)`: The Firefox WebDriver instance.
- `_downloads_directory (str)`: The directory path for downloads.

Methods (Firefox):
- `__init__(self) -> None`: Initializes a Firefox instance and sets the
    downloads directory.
- `_get_driver(self) -> webdriver.Firefox`: Gets a Firefox WebDriver instance
    with specified options.
- `_get_firefox_options(
        self,
        firefox_executable_path: Optional[str] = None
    ) -> webdriver.FirefoxOptions`: Gets Firefox WebDriver options with
    specified configurations.

For detailed documentation and examples, please refer to the package
documentation.
"""

from typing import Optional

from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager

from scriptman._directories import DirectoryHandler
from scriptman._selenium_interactions import SeleniumInteractionHandler
from scriptman._settings import Settings


class Firefox(SeleniumInteractionHandler):
    """
    Firefox manages the creation of Firefox Selenium WebDriver instances.

    Attributes:
        driver (webdriver.Firefox): The Firefox WebDriver instance.
        _downloads_directory (str): The directory path for downloads.
    """

    def __init__(self) -> None:
        """
        Initialize Firefox instance and set the downloads directory.
        """
        self._downloads_directory = DirectoryHandler().downloads_dir
        self._driver = self._get_driver()
        super().__init__(self._driver)

    def _get_driver(self) -> webdriver.Firefox:
        """
        Get a Firefox WebDriver instance with specified options.

        Returns:
            webdriver.Firefox: A Firefox WebDriver instance.
        """
        options = self._get_firefox_options()
        service = Service(GeckoDriverManager().install())
        return webdriver.Firefox(options=options, service=service)

    def _get_firefox_options(
        self,
        firefox_executable_path: Optional[str] = None,
    ) -> webdriver.FirefoxOptions:
        """
        Get Firefox WebDriver options with specified configurations.

        Args:
            firefox_executable_path (str, optional): Path to the Firefox binary
                executable.

        Returns:
            webdriver.FirefoxOptions: Firefox WebDriver options.
        """
        options = webdriver.FirefoxOptions()

        if firefox_executable_path:
            options.binary_location = firefox_executable_path

        if Settings.selenium_optimizations and not Settings.debug_mode:
            optimization_args = [
                "--headless",
                "--disable-infobars",
                "--disable-extensions",
                "--disable-notifications",
                "--remote-debugging-port=9222",
            ]
            [options.add_argument(arg) for arg in optimization_args]

        preferences = {
            "browser.download.folderList": 2,
            "browser.download.dir": self._downloads_directory,
            "browser.helperApps.neverAsk.saveToDisk": (
                "application/"
                "octet-stream,application/"
                "pdf,text/"
                "plain,text/"
                "csv"
            ),
        }
        [options.set_preference(n, v) for n, v in preferences.items()]
        return options
