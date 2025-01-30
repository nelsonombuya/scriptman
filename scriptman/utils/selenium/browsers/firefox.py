from typing import Optional

from selenium import webdriver
from selenium.webdriver import Firefox as FirefoxDriver
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager

from scriptman.core.config import config
from scriptman.utils.selenium._enums import SeleniumBrowser


class Firefox(SeleniumBrowser[FirefoxDriver]):
    def __init__(self) -> None:
        self._driver = self._get_driver()

    @property
    def driver(self) -> FirefoxDriver:
        return self._driver

    def _get_driver(self) -> webdriver.Firefox:
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

        if config.env.get("selenium_optimizations"):
            [
                options.add_argument(arg)
                for arg in [
                    "--headless",
                    "--disable-infobars",
                    "--disable-extensions",
                    "--disable-notifications",
                    "--remote-debugging-port=9222",
                ]
            ]

        [
            options.set_preference(preference, value)
            for preference, value in {
                "browser.download.folderList": 2,
                "browser.download.dir": config.env.downloads_dir,
                "browser.helperApps.neverAsk.saveToDisk": (
                    "application/"
                    "octet-stream,application/"
                    "pdf,text/"
                    "plain,text/"
                    "csv"
                ),
            }.items()
        ]
        return options
