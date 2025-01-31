from os import name
from pathlib import Path
from platform import architecture, machine, system
from shutil import rmtree
from typing import Literal, Optional
from zipfile import ZipFile

from loguru import Logger, logger
from requests import get
from selenium.webdriver import Chrome as ChromeDriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from scriptman.core.config import config
from scriptman.powers.selenium._enums import SeleniumBrowser


class Chrome(SeleniumBrowser[ChromeDriver]):
    _local_mode: bool = False

    def _get_driver(self) -> ChromeDriver:
        """
        🏎 Get the Chrome WebDriver instance associated with the current browser.

        Returns:
            Driver: The Chrome WebDriver instance.
        """
        try:
            if self._local_mode:
                raise ValueError("Setting up Chrome in Local mode...")
            options = self._get_chrome_options()
            service = Service(ChromeDriverManager().install())
        except ValueError:
            self.log.debug("Setting up Chrome in Local mode...")
            cd = ChromeDownloader()
            chrome_version = config.env.get("chrome_version", 126)
            chrome_browser = cd.download(chrome_version, "browser")
            chrome_driver = cd.download(chrome_version, "driver")
            options = self._get_chrome_options(chrome_browser)
            service = Service(executable_path=chrome_driver)
        return ChromeDriver(options, service)

    def _get_chrome_options(
        self, chrome_executable_path: Optional[Path] = None
    ) -> ChromeOptions:
        """
        ⚙ Get Chrome WebDriver options with specified configurations.

        Args:
            chrome_executable_path (Path, optional): Path to the Chrome binary executable.

        Returns:
            ChromeOptions: Chrome WebDriver options.
        """
        options = ChromeOptions()

        if chrome_executable_path:
            options.binary_location = chrome_executable_path.resolve().as_posix()

        if config.env.get("selenium_optimizations"):
            [
                options.add_argument(arg)
                for arg in [
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
            ]

        options.add_experimental_option(
            "prefs",
            {
                "download.directory_upgrade": True,
                "download.safebrowsing.enabled": True,
                "download.prompt_for_download": False,
                "download.default_directory": config.env.downloads_dir,
            },
        )

        return options


class ChromeDownloader:
    """
    ⬇ ChromeDownloader is responsible for downloading and managing the Chrome Browser
    and Driver.
    """

    log: Logger = logger.bind(handler="Chrome Downloader")
    chrome_download_dir: Path = Path(config.env.downloads_dir, "..", "selenium")

    def download(self, version: int, app: Literal["driver", "browser"]) -> Path:
        """
        ⬇ Download the Chrome Driver/Browser for the specified Chrome version.

        Args:
            version (int): The desired Chrome version.
            app (str): The application name (default is "driver").

        Returns:
            str: The path to the downloaded ChromeDriver executable.
        """
        if app_path := self._app_already_downloaded(version, app):
            self.log.info(f"Found {str(app).title()} v{version} at {app_path}")
            return app_path

        self.log.debug(f"Downloading {str(app).title()} v{version}")
        download_urls = self._fetch_download_urls()
        url: Optional[str] = None

        for version_info in download_urls["versions"]:
            if version_info["version"].startswith(str(version)):
                url = self._get_app_url(version_info, app)
                break

        if url:
            return self._get_app_path(url, app, version)
        else:
            raise KeyError(f"No {str(app).title()} URL for Chrome version {version}. ")

    def _app_already_downloaded(
        self, version: int, app: Literal["driver", "browser"]
    ) -> Optional[Path]:
        """
        🔍 Check if the specified Chrome application is already downloaded.

        Args:
            version (int): The desired Chrome version.
            app (str): The application name (default is "driver").

        Returns:
            Optional[Path]: The path to the downloaded file if it exists, None otherwise.
        """
        app_name: str = "chromedriver" if app == "driver" else "chrome"
        suffix: str = ".exe" if name == "nt" else ""
        path: Path = Path(
            self.chrome_download_dir,
            f"{app}-{self._get_system_platform()}-{version}",
            app_name + suffix,
        )

        return (
            path
            if path.exists() and path.parent.stem.split("-")[2] == str(version)
            else None
        )

    def _fetch_download_urls(self) -> dict:
        """
        📩 Fetch and return Chrome download URLs.

        Returns:
            dict: JSON data containing download URLs.
        """
        self.log.debug("Fetching Chrome download URLs...")
        response = get(config.env.chrome_download_url)
        response.raise_for_status()
        return response.json()

    def _get_app_url(
        self, version_info: dict, app: Literal["driver", "browser"]
    ) -> Optional[str]:
        """
        🔗 Get the download URL for the specified Chrome version and platform.

        Args:
            version_info (dict): Information about Chrome versions and downloads.
            app (str): The application name (default is "driver").

        Returns:
            Optional[str]: The download URL or None if not found.
        """
        current_platform = self._get_system_platform()
        if current_platform:
            for download_info in version_info["downloads"].get(app, []):
                if download_info["platform"] == current_platform:
                    self.log.debug(f"Found {str(app).title()} URL for {current_platform}")
                    return download_info["url"]

    def _get_system_platform(self) -> str:
        """
        🆔 Get the platform identifier based on the current system.

        Returns:
            str: The platform identifier.
        """
        system_platform = {
            "Linux": "linux64",
            "Darwin": "mac-x64" if machine() == "x86_64" else "mac-arm64",
            "Windows": "win32" if architecture()[0] == "32bit" else "win64",
        }.get(system())

        if not system_platform:
            raise Exception("Invalid System Platform!")
        self.log.debug(f"System Platform: {system_platform}")
        return system_platform

    def _get_app_path(
        self, url: str, app: Literal["driver", "browser"], version: int
    ) -> Path:
        """
        🚶🏾‍♂️ Get the path to the Chrome Driver or Browser executable, downloading it if
        necessary.

        Args:
            url (str): The URL to download Chrome Driver/Browser from.
            app (str): The application name (default is "driver").
            version (int): The desired Chrome version.

        Returns:
            Path: The path to the downloaded ChromeDriver executable.
        """
        app_name: str = "chromedriver" if app == "driver" else "chrome"
        suffix: str = ".exe" if name == "nt" else ""
        path: Path = Path(
            self.chrome_download_dir,
            f"{app}-{self._get_system_platform()}-{version}",
            app_name + suffix,
        )
        return self._download_and_extract_app(url, app, path)

    def _download_and_extract_app(
        self, url: str, app: Literal["driver", "browser"], path: Path
    ) -> Path:
        """
        🗃 Download and extract the Chrome Driver/Browser executable from the
        given URL.

        Args:
            url (str): The URL to download Chrome Driver/Browser from.
            app (str): The application name (default is "chromedriver").
            path (Path): The path to save the downloaded ChromeDriver executable.

        Returns:
            Path: The path to the downloaded ChromeDriver executable.
        """
        self.log.debug(f"Downloading {app} from {url}")
        response = get(url)
        response.raise_for_status()
        zip_download_path = Path(
            path.parent, f"chrome{'driver' if app == 'chromedriver' else ''}.zip"
        )
        zip_download_path.mkdir(parents=True, exist_ok=True)

        with open(zip_download_path, "wb") as file:
            self.log.debug(f"Writing {app} to {zip_download_path}")
            file.write(response.content)

        with ZipFile(zip_download_path, "r") as zip_ref:
            self.log.debug(f"Extracting {app} to {zip_download_path.parent}")
            zip_ref.extractall(zip_download_path.parent)

        zip_download_path.unlink()  # Remove the downloaded zip file
        self.log.debug(f"Downloaded {app} to {path.parent}")
        return path

    @classmethod
    def cleanup_chrome_downloads(cls) -> None:
        """🧹 Clean up Chrome downloads."""
        cls.log.debug("Cleaning up Chrome downloads...")
        rmtree(cls.chrome_download_dir)
