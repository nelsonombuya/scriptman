import os
import platform
from typing import Optional
from zipfile import ZipFile

import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from .directories import DirectoryHandler
from .logs import LogHandler, LogLevel
from .selenium_interactions import SeleniumInteractionHandler
from .settings import settings


class ChromeApp:
    CHROME = "chrome"
    CHROMEDRIVER = "chromedriver"


class Chrome(SeleniumInteractionHandler):
    """
    Chrome manages the creation of Chrome Selenium WebDriver instances.

    Attributes:
        driver (webdriver.Chrome): The Chrome WebDriver instance.
        _downloads_directory (str): The directory path for downloads.
    """

    def __init__(self) -> None:
        """
        Initialize Chrome instance and set the downloads directory.
        """
        self._downloads_directory = DirectoryHandler().downloads_dir
        self.driver = self._get_driver()
        super().__init__(self.driver)

    def _get_driver(self) -> webdriver.Chrome:
        """
        Get a Chrome WebDriver instance with specified options.

        Returns:
            webdriver.Chrome: A Chrome WebDriver instance.
        """
        try:
            if settings.selenium_custom_driver_mode:
                raise ValueError
            options = self._get_chrome_options()
            service = Service(ChromeDriverManager().install())
        except ValueError:
            cdm = ChromeDownloadHandler()
            chrome_version = settings.selenium_custom_driver_version
            chrome_driver = cdm.download(chrome_version)
            chrome_browser = cdm.download(chrome_version, ChromeApp.CHROME)
            options = self._get_chrome_options(chrome_browser)
            service = Service(executable_path=chrome_driver)
        return webdriver.Chrome(options, service)

    def _get_chrome_options(
        self,
        chrome_executable_path: Optional[str] = None,
    ) -> webdriver.ChromeOptions:
        """
        Get Chrome WebDriver options with specified configurations.

        Args:
            chrome_executable_path (str, optional): Path to the Chrome binary
                executable.

        Returns:
            webdriver.ChromeOptions: Chrome WebDriver options.
        """
        options = webdriver.ChromeOptions()

        if chrome_executable_path:
            options.binary_location = chrome_executable_path

        if settings.selenium_optimizations_mode and not settings.debug_mode:
            optimization_args = [
                "--headless",
                "--no-sandbox",
                "--mute-audio",
                "--disable-gpu",
                "--disable-infobars",
                "--disable-extensions",
                "--disable-dev-shm-usage",
                "--disable-notifications",
                "--disable-setuid-sandbox",
                "--remote-debugging-port=9222",
                "--disable-browser-side-navigation",
                "--disable-blink-features=AutomationControlled",
            ]
            [options.add_argument(arg) for arg in optimization_args]

        options.add_experimental_option(
            "prefs",
            {
                "download.directory_upgrade": True,
                "download.safebrowsing.enabled": True,
                "download.prompt_for_download": False,
                "download.default_directory": self._downloads_directory,
            },
        )

        return options


class ChromeDownloadHandler:
    """
    ChromeDownloadHandler is responsible for downloading and managing the
    Chrome Browser and Driver.

    Attributes:
        _log (LogHandler): The log handler instance for logging.
        _downloads_dir (str): The directory path for downloads.
    """

    def __init__(self) -> None:
        """
        Initialize ChromeDownloadHandler instance and set directories.
        """
        self._log = LogHandler("Chrome Download Manager")
        self._selenium_dir = DirectoryHandler().selenium_dir

    def download(
        self,
        version: int,
        app: str = ChromeApp.CHROMEDRIVER,
    ) -> str:
        """
        Download the Chrome Driver/Browser for the specified Chrome version.

        Args:
            version (int): The desired Chrome version.
            app (str): The application name (default is "chromedriver").

        Returns:
            str: The path to the downloaded ChromeDriver executable.
        """
        self._log.message(f"Downloading {str(app).title()} v{version}")
        download_urls = self._fetch_download_urls()
        url = None

        try:
            for version_info in download_urls["versions"]:
                if version_info["version"].startswith(str(version)):
                    url = self._get_app_url(version_info, app)
                    break
            if url:
                return self._get_app_path(url, app)
            else:
                raise KeyError
        except KeyError:
            self._log.message(
                (
                    f"No {str(app).title()} URL for Chrome version {version}. "
                    f"Increasing Chrome version to {version + 1}"
                ),
                LogLevel.WARN,
            )
            return self.download(version + 1)

    def _fetch_download_urls(self) -> dict:
        """
        Fetch and return Chrome download URLs.

        Returns:
            dict: JSON data containing download URLs.
        """
        response = requests.get(settings.selenium_chrome_url)
        response.raise_for_status()
        return response.json()

    def _get_app_url(
        self,
        version_info: dict,
        app: str = ChromeApp.CHROMEDRIVER,
    ) -> Optional[str]:
        """
        Get the download URL for the specified Chrome version and platform.

        Args:
            version_info (dict): Information about Chrome versions and
            downloads.
            app (str): The application name (default is "chromedriver").

        Returns:
            Optional[str]: The download URL or None if not found.
        """
        current_platform = self._get_system_platform()
        if current_platform:
            for download_info in version_info["downloads"].get(app, []):
                if download_info["platform"] == current_platform:
                    return download_info["url"]

    def _get_system_platform(self) -> str:
        """
        Get the platform identifier based on the current system.

        Returns:
            str: The platform identifier.
        """
        system = platform.system()
        machine = platform.machine()
        architecture = platform.architecture()[0]
        return {
            "Linux": "linux64",
            "Darwin": "mac-x64" if machine == "x86_64" else "mac-arm64",
            "Windows": "win32" if architecture == "32bit" else "win64",
        }.get(system, "unknown")

    def _get_app_path(
        self,
        url: str,
        app: str = ChromeApp.CHROMEDRIVER,
    ) -> str:
        """
        Get the path to the Chrome Driver or Browser executable, downloading
        it if necessary.

        Args:
            url (str): The URL to download Chrome Driver/Browser from.
            app (str): The application name (default is "chromedriver").

        Returns:
            str: The path to the Chrome Driver/Browser executable.
        """
        filename = (
            "chromedriver.exe"
            if os.name == "nt" and app == "chromedriver"
            else "chromedriver"
            if app == "chromedriver"
            else "chrome"
            if not os.name == "nt" and app == "chrome"
            else "chrome.exe"
        )
        path = os.path.join(
            self._selenium_dir,
            f"{app}-{self._get_system_platform()}",
            filename,
        )

        if not os.path.exists(path):
            self._download_and_extract_app(url, app)

        return path

    def _download_and_extract_app(
        self,
        url: str,
        app: str = ChromeApp.CHROMEDRIVER,
    ) -> str:
        """
        Download and extract the Chrome Driver/Browser executable from the
        given URL.

        Args:
            url (str): The URL to download Chrome Driver/Browser from.
            app (str): The application name (default is "chromedriver").

        Returns:
            str: Path of the downloaded driver/browser.
        """
        response = requests.get(url)
        response.raise_for_status()
        zip_download_path = os.path.join(
            self._selenium_dir,
            f"chrome{'driver' if app == 'chromedriver' else ''}.zip",
        )

        with open(zip_download_path, "wb") as file:
            file.write(response.content)

        with ZipFile(zip_download_path, "r") as zip_ref:
            zip_ref.extractall(self._selenium_dir)

        return os.path.join(
            self._selenium_dir,
            f"{app}-{self._get_system_platform()}",
        )
