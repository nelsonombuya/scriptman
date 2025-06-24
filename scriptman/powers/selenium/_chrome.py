try:
    from os import name
    from pathlib import Path
    from platform import architecture, machine, system
    from shutil import rmtree
    from tempfile import gettempdir, mkdtemp
    from typing import Any, Literal, Optional
    from zipfile import ZipFile

    from loguru import logger
    from requests import get
    from selenium.webdriver import Chrome as ChromeDriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    from scriptman.core.config import config
    from scriptman.powers.selenium._enums import SeleniumBrowser
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[selenium]."
    )


class Chrome(SeleniumBrowser[ChromeDriver]):
    def _get_driver(self) -> ChromeDriver:
        """
        🏎 Get the Chrome WebDriver instance associated with the current browser.

        Returns:
            Driver: The Chrome WebDriver instance.
        """
        if config.settings.get("selenium_local_mode", True):
            options, service = self._get_local_mode_options()
        else:
            options, service = self._get_non_local_mode_options()
        return ChromeDriver(options=options, service=service)  # type: ignore

    def _get_non_local_mode_options(self) -> tuple[ChromeOptions, Service]:
        """
        ⚙ Get Chrome WebDriver options with specified configurations.
        """
        self.log.debug("Setting up Chrome in Non-Local mode...")
        options = self._get_chrome_options()
        service = Service(ChromeDriverManager().install())
        return options, service

    def _get_local_mode_options(self) -> tuple[ChromeOptions, Service]:
        """
        ⚙ Get Chrome WebDriver options with specified configurations.
        """
        self.log.debug("Setting up Chrome in Local mode...")
        cd = ChromeDownloader()
        chrome_version = config.settings.get("selenium_chrome_version", 138)
        chrome_driver = cd.download(chrome_version, "chromedriver")
        chrome_browser = cd.download(chrome_version, "chrome")
        options = self._get_chrome_options(chrome_browser)
        service = Service(executable_path=chrome_driver)
        return options, service

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
        download_dir = Path(config.settings.downloads_dir).resolve().as_posix()

        if chrome_executable_path:
            options.binary_location = chrome_executable_path.resolve().as_posix()

        if config.settings.get("selenium_optimizations", False):
            for arg in [
                "--headless" if config.settings.get("selenium_headless", True) else None,
                "--no-sandbox",
                "--mute-audio",
                "--disable-gpu",
                "--disable-infobars",
                "--disable-extensions",
                "--disable-dev-shm-usage",
                "--disable-notifications",
                "--disable-setuid-sandbox",
                "--disable-software-rasterizer",
                "--disable-features=TranslateUI",
                "--disable-renderer-backgrounding",
                "--disable-ipc-flooding-protection",
                "--disable-browser-side-navigation",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-blink-features=AutomationControlled",
            ]:
                if arg is not None:
                    options.add_argument(arg)

        options.add_experimental_option(
            "prefs",
            {
                "download.directory_upgrade": True,
                "download.safebrowsing.enabled": True,
                "download.prompt_for_download": False,
                "download.default_directory": download_dir,
            },
        )

        return options


class ChromeDownloader:
    """
    ⬇ ChromeDownloader is responsible for downloading and managing the Chrome Browser
    and Driver.
    """

    log = logger.bind(name="Chrome Downloader")
    _chrome_download_dir: Optional[Path] = None

    @property
    def chrome_download_dir(self) -> Path:
        """
        📁 Get the Chrome download directory with fallback to temp directory.

        Returns:
            Path: The Chrome download directory path.
        """
        if self._chrome_download_dir is None:
            # Try primary downloads directory first
            primary_dir = Path(config.settings.downloads_dir, ".selenium", "chrome")
            try:
                primary_dir.mkdir(parents=True, exist_ok=True)
                test_file = primary_dir / ".test_write"  # Test write permissions
                test_file.touch()
                test_file.unlink()
                self._chrome_download_dir = primary_dir
                self.log.debug(f"Using primary download directory: {primary_dir}")
            except (PermissionError, OSError) as e:
                # Fallback to temp directory
                temp_dir = Path(mkdtemp(prefix="scriptman_chrome_", dir=gettempdir()))
                self._chrome_download_dir = temp_dir
                self.log.warning(
                    f"Permission denied for primary directory {primary_dir}: {e}. "
                    f"Using temporary directory: {temp_dir}"
                )

        return self._chrome_download_dir

    def download(self, version: int, app: Literal["chromedriver", "chrome"]) -> Path:
        """
        ⬇ Download the Chrome Driver/Browser for the specified Chrome version.

        Args:
            version (int): The desired Chrome version.
            app (str): The application name (default is "chromedriver").

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
            if str(version_info["version"]).startswith(str(version)):
                url = self._get_app_url(version_info, app)
                break

        if url:
            return self._download_and_extract_app(url, app, version)
        else:
            raise KeyError(f"No {str(app).title()} URL for Chrome version {version}. ")

    def _find_executable_in_directory(
        self, target_dir: Path, app: Literal["chromedriver", "chrome"]
    ) -> Optional[Path]:
        """
        🔍 Find the executable file in the given directory or its subdirectories.

        Args:
            target_dir (Path): The directory to search in.
            app (str): The application name (chromedriver or chrome).

        Returns:
            Optional[Path]: The path to the executable if found, None otherwise.
        """
        executable_name = app + ".exe" if name == "nt" else app

        # First, check if the executable is directly in the target directory
        direct_path = target_dir / executable_name
        if direct_path.exists():
            self.log.debug(f"Found {app} executable at {direct_path}")
            return direct_path

        # If not found directly, look for it in subdirectories
        for item in target_dir.iterdir():
            if item.is_dir():
                # Check if this subdirectory contains the executable
                subdir_executable = item / executable_name
                if subdir_executable.exists():
                    self.log.debug(f"Found {app} executable at {subdir_executable}")
                    return subdir_executable

                # Also check for any executable with the app name in this subdirectory
                for subitem in item.iterdir():
                    if subitem.is_file() and subitem.name.startswith(app):
                        if name == "nt" and subitem.suffix == ".exe":
                            self.log.debug(f"Found {app} executable at {subitem}")
                            return subitem
                        elif name != "nt" and subitem.suffix == "":
                            self.log.debug(f"Found {app} executable at {subitem}")
                            return subitem

        return None

    def _app_already_downloaded(
        self, version: int, app: Literal["chromedriver", "chrome"]
    ) -> Optional[Path]:
        """
        🔍 Check if the specified Chrome application is already downloaded.

        Args:
            version (int): The desired Chrome version.
            app (str): The application name (default is "chromedriver").

        Returns:
            Optional[Path]: The path to the downloaded file if it exists, None otherwise.
        """
        target_dir = Path(
            self.chrome_download_dir,
            f"{app}-{self._get_system_platform()}-{version}",
        )

        if not target_dir.exists():
            return None

        executable_path = self._find_executable_in_directory(target_dir, app)
        if executable_path and executable_path.exists():
            self.log.debug(f"Found existing {app} v{version} at {executable_path}")
            return executable_path

        return None

    def _fetch_download_urls(self) -> dict[str, Any]:
        """
        📩 Fetch and return Chrome download URLs.

        Returns:
            dict: JSON data containing download URLs.
        """
        self.log.debug("Fetching Chrome download URLs...")
        response = get(config.settings.selenium_chrome_download_url)
        response.raise_for_status()
        return dict(response.json())

    def _get_app_url(
        self, version_info: dict[str, Any], app: Literal["chromedriver", "chrome"]
    ) -> Optional[str]:
        """
        🔗 Get the download URL for the specified Chrome version and platform.

        Args:
            version_info (dict): Information about Chrome versions and downloads.
            app (str): The application name (default is "chromedriver").

        Returns:
            Optional[str]: The download URL or None if not found.
        """
        current_platform = self._get_system_platform()
        if current_platform:
            for download_info in dict(version_info["downloads"]).get(app, []):
                if download_info["platform"] == current_platform:
                    self.log.debug(f"Found {str(app).title()} URL for {current_platform}")
                    return str(download_info["url"])
        return None

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

    def _download_and_extract_app(
        self, url: str, app: Literal["chromedriver", "chrome"], version: int
    ) -> Path:
        """
        🗃 Download and extract the Chrome Driver/Browser executable from the
        given URL.

        Args:
            url (str): The URL to download Chrome Driver/Browser from.
            app (str): The application name (default is "chromedriver").
            version (int): The Chrome version.

        Returns:
            Path: The path to the downloaded ChromeDriver executable.
        """
        self.log.debug(f"Downloading {app} from {url}")
        response = get(url)
        response.raise_for_status()

        target_dir = Path(
            self.chrome_download_dir,
            f"{app}-{self._get_system_platform()}-{version}",
        )
        target_dir.mkdir(parents=True, exist_ok=True)
        zip_download_path = target_dir / f"{app}.zip"

        with open(zip_download_path, "wb") as file:
            self.log.debug(f"Writing {app} to {zip_download_path}")
            file.write(response.content)

        with ZipFile(zip_download_path, "r") as zip_ref:
            self.log.debug(f"Extracting {app} to {target_dir}")
            zip_ref.extractall(target_dir)

        zip_download_path.unlink()
        if executable_path := self._find_executable_in_directory(target_dir, app):
            return executable_path
        raise FileNotFoundError(
            f"Could not find {app} executable in extracted contents at {target_dir}"
        )

    @classmethod
    def cleanup_chrome_downloads(cls) -> None:
        """🧹 Clean up Chrome downloads."""
        cls.log.debug("Cleaning up Chrome downloads...")
        try:
            primary_dir = Path(config.settings.downloads_dir, ".selenium", "chrome")
            if primary_dir.exists():
                rmtree(primary_dir)
                cls.log.debug(f"Cleaned up primary directory: {primary_dir}")
        except Exception as e:
            cls.log.warning(f"Failed to clean up primary directory: {e}")

        # Note: We don't clean up temp directories automatically as they might be in use
        # The OS will clean them up eventually, or they can be cleaned manually

    @classmethod
    def get_download_info(cls) -> dict[str, Any]:
        """
        📊 Get information about Chrome download directories.

        Returns:
            dict: Information about download directories and their status.
        """
        primary_dir = Path(config.settings.downloads_dir, ".selenium", "chrome")
        temp_base = Path(gettempdir())

        # Find any existing temp directories
        temp_dirs = [
            d
            for d in temp_base.iterdir()
            if d.is_dir() and d.name.startswith("scriptman_chrome_")
        ]

        return {
            "primary_directory": {
                "path": str(primary_dir),
                "exists": primary_dir.exists(),
                "writable": cls._is_directory_writable(primary_dir),
            },
            "temp_directories": [
                {
                    "path": str(temp_dir),
                    "exists": temp_dir.exists(),
                    "writable": cls._is_directory_writable(temp_dir),
                }
                for temp_dir in temp_dirs
            ],
            "temp_base": str(temp_base),
        }

    @staticmethod
    def _is_directory_writable(directory: Path) -> bool:
        """
        🔍 Check if a directory is writable.

        Args:
            directory (Path): Directory to check.

        Returns:
            bool: True if writable, False otherwise.
        """
        if not directory.exists():
            return False
        try:
            test_file = directory / ".test_write"
            test_file.touch()
            test_file.unlink()
            return True
        except (PermissionError, OSError):
            return False
