try:
    import tarfile
    from os import name
    from pathlib import Path
    from platform import architecture, machine, system
    from shutil import rmtree
    from tempfile import gettempdir, mkdtemp
    from typing import Any, Optional
    from zipfile import ZipFile

    from loguru import logger
    from requests import get
    from selenium.webdriver import Firefox as FirefoxDriver
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    from selenium.webdriver.firefox.service import Service
    from webdriver_manager.firefox import GeckoDriverManager

    from scriptman.core.config import config
    from scriptman.powers.selenium._utils import SeleniumBrowser
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[selenium]."
    )


class Firefox(SeleniumBrowser[FirefoxDriver]):
    def _get_driver(self) -> FirefoxDriver:
        """
        🦊 Get the Firefox WebDriver instance associated with the current browser.

        Returns:
            Driver: The Firefox WebDriver instance.
        """
        if config.settings.get("selenium_managed_mode", True):
            options, service = self._get_managed_mode_options()
        else:
            options, service = self._get_non_managed_mode_options()
        return FirefoxDriver(options=options, service=service)  # type: ignore

    def _get_non_managed_mode_options(self) -> tuple[FirefoxOptions, Service]:
        """
        ⚙ Get Firefox WebDriver options with specified configurations.
        """
        self.log.debug("Setting up Firefox in Non-Managed mode...")
        options = self._get_firefox_options()
        service = Service(GeckoDriverManager().install())
        return options, service

    def _get_managed_mode_options(self) -> tuple[FirefoxOptions, Service]:
        """
        ⚙ Get Firefox WebDriver options with specified configurations.
        """
        self.log.debug("Setting up Firefox in Managed mode...")
        fd = FirefoxDownloader()
        firefox_version = config.settings.get("selenium_firefox_version", "latest")
        geckodriver_path = fd.download_geckodriver(firefox_version)
        firefox_browser = fd.download_firefox(firefox_version)
        options = self._get_firefox_options(firefox_browser)
        service = Service(executable_path=geckodriver_path)
        return options, service

    def _get_firefox_options(
        self, firefox_executable_path: Optional[Path] = None
    ) -> FirefoxOptions:
        """
        ⚙ Get Firefox WebDriver options with specified configurations.

        Args:
            firefox_executable_path (Path, optional): Path to the Firefox binary
                executable.

        Returns:
            FirefoxOptions: Firefox WebDriver options.
        """
        options = FirefoxOptions()

        if firefox_executable_path:
            options.binary_location = firefox_executable_path.resolve().as_posix()

        if config.settings.get("selenium_optimizations", False):
            firefox_args = [
                "--headless" if config.settings.get("selenium_headless", True) else None,
                "--no-sandbox",
                "--disable-gpu",
                "--disable-extensions",
                "--disable-dev-shm-usage",
                "--disable-notifications",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor",
            ]

            for arg in firefox_args:
                if arg is not None:
                    options.add_argument(arg)

        # Firefox preferences for downloads
        # Use default download directory
        options.set_preference("browser.download.folderList", 1)
        options.set_preference("browser.download.manager.showWhenStarting", False)
        options.set_preference(
            "browser.helperApps.neverAsk.saveToDisk",
            "application/pdf,application/zip,text/csv,application/vnd.ms-excel",
        )

        return options

    def __del__(self) -> None:
        """
        🧹 Clean up Firefox downloads on garbage collection if enabled.
        """
        if config.settings.get("selenium_cleanup_downloads_on_exit", False):
            try:
                self.log.debug("Cleaning up Firefox downloads on exit...")
                FirefoxDownloader.cleanup_firefox_downloads()
            except Exception as e:
                self.log.warning(f"Failed to clean up Firefox downloads: {e}")


class FirefoxDownloader:
    """
    ⬇ FirefoxDownloader is responsible for downloading and managing the Firefox Browser
    and GeckoDriver.
    """

    log = logger.bind(name="Firefox Downloader")
    __firefox_download_dir: Optional[Path] = None

    @property
    def firefox_download_dir(self) -> Path:
        """
        📁 Get the Firefox download directory with fallback to temp directory.

        Returns:
            Path: The Firefox download directory path.
        """
        if self.__firefox_download_dir is None:
            # Try primary downloads directory first
            primary_dir = Path(config.settings.downloads_dir, ".selenium", "firefox")
            try:
                primary_dir.mkdir(parents=True, exist_ok=True)
                test_file = primary_dir / ".test_write"  # Test write permissions
                test_file.touch()
                test_file.unlink()
                self.__firefox_download_dir = primary_dir
                self.log.debug(f"Using primary download directory: {primary_dir}")
            except (PermissionError, OSError) as e:
                # Fallback to temp directory
                temp_dir = Path(mkdtemp(prefix="scriptman_firefox_", dir=gettempdir()))
                self.__firefox_download_dir = temp_dir
                self.log.warning(
                    f"Permission denied for primary directory {primary_dir}: {e}. "
                    f"Using temporary directory: {temp_dir}"
                )

        return self.__firefox_download_dir

    def download_firefox(self, version: str = "latest") -> Path:
        """
        ⬇ Download the Firefox Browser for the specified version.

        Args:
            version (str): The desired Firefox version. Defaults to "latest".

        Returns:
            Path: The path to the downloaded Firefox executable.
        """
        if firefox_path := self._firefox_already_downloaded(version):
            self.log.info(f"Found Firefox {version} at {firefox_path}")
            return firefox_path

        self.log.debug(f"Downloading Firefox {version}")
        url = self._get_firefox_download_url(version)
        return self._download_and_extract_firefox(url, version)

    def download_geckodriver(self, firefox_version: str = "latest") -> Path:
        """
        ⬇ Download the GeckoDriver for the specified Firefox version.

        Args:
            firefox_version (str): The Firefox version to get compatible GeckoDriver for.

        Returns:
            Path: The path to the downloaded GeckoDriver executable.
        """
        if geckodriver_path := self._geckodriver_already_downloaded():
            self.log.info(f"Found GeckoDriver at {geckodriver_path}")
            return geckodriver_path

        self.log.debug("Downloading GeckoDriver")
        url = self._get_geckodriver_download_url()
        return self._download_and_extract_geckodriver(url)

    def _firefox_already_downloaded(self, version: str) -> Optional[Path]:
        """
        🔍 Check if the specified Firefox version is already downloaded.

        Args:
            version (str): The desired Firefox version.

        Returns:
            Optional[Path]: The path to the downloaded file if it exists, None otherwise.
        """
        target_dir = Path(
            self.firefox_download_dir,
            f"firefox-{self._get_system_platform()}-{version}",
        )

        if not target_dir.exists():
            return None

        executable_path = self._find_firefox_executable(target_dir)
        if executable_path and executable_path.exists():
            self.log.debug(f"Found existing Firefox {version} at {executable_path}")
            return executable_path

        return None

    def _geckodriver_already_downloaded(self) -> Optional[Path]:
        """
        🔍 Check if GeckoDriver is already downloaded.

        Returns:
            Optional[Path]: The path to the downloaded file if it exists, None otherwise.
        """
        target_dir = Path(
            self.firefox_download_dir,
            f"geckodriver-{self._get_system_platform()}",
        )

        if not target_dir.exists():
            return None

        executable_name = "geckodriver.exe" if name == "nt" else "geckodriver"
        executable_path = target_dir / executable_name

        if executable_path.exists():
            self.log.debug(f"Found existing GeckoDriver at {executable_path}")
            return executable_path

        return None

    def _find_firefox_executable(self, target_dir: Path) -> Optional[Path]:
        """
        🔍 Find the Firefox executable file in the given directory or its subdirectories.

        Args:
            target_dir (Path): The directory to search in.

        Returns:
            Optional[Path]: The path to the executable if found, None otherwise.
        """
        if name == "nt":  # Windows
            executable_names = ["firefox.exe"]
        elif system() == "Darwin":  # macOS
            executable_names = ["firefox", "Firefox.app/Contents/MacOS/firefox"]
        else:  # Linux
            executable_names = ["firefox"]

        # First, check direct paths
        for exe_name in executable_names:
            direct_path = target_dir / exe_name
            if direct_path.exists():
                self.log.debug(f"Found Firefox executable at {direct_path}")
                return direct_path

        # Then search in subdirectories
        for item in target_dir.rglob("*"):
            if item.is_file():
                for exe_name in executable_names:
                    if item.name == Path(exe_name).name:
                        self.log.debug(f"Found Firefox executable at {item}")
                        return item

        return None

    def _get_firefox_download_url(self, version: str) -> str:
        """
        🔗 Get the download URL for the specified Firefox version and platform.

        Args:
            version (str): The Firefox version.

        Returns:
            str: The download URL.
        """
        platform = self._get_system_platform()

        if version == "latest":
            # Mozilla's direct download URLs for latest version
            platform_urls = {
                "win64": (
                    "https://download.mozilla.org/?product=firefox-latest"
                    "&os=win64&lang=en-US"
                ),
                "win32": (
                    "https://download.mozilla.org/?product=firefox-latest"
                    "&os=win&lang=en-US"
                ),
                "linux64": (
                    "https://download.mozilla.org/?product=firefox-latest"
                    "&os=linux64&lang=en-US"
                ),
                "mac-x64": (
                    "https://download.mozilla.org/?product=firefox-latest"
                    "&os=osx&lang=en-US"
                ),
                "mac-arm64": (
                    "https://download.mozilla.org/?product=firefox-latest"
                    "&os=osx&lang=en-US"
                ),
            }
        else:
            # For specific versions, use archive URLs
            base_archive = "https://archive.mozilla.org/pub/firefox/releases"
            platform_urls = {
                "win64": f"{base_archive}/{version}/win64/en-US/firefox-{version}.exe",
                "win32": f"{base_archive}/{version}/win32/en-US/firefox-{version}.exe",
                "linux64": (
                    f"{base_archive}/{version}/linux-x86_64/en-US/"
                    f"firefox-{version}.tar.bz2"
                ),
                "mac-x64": (f"{base_archive}/{version}/mac/en-US/Firefox {version}.dmg"),
                "mac-arm64": (
                    f"{base_archive}/{version}/mac/en-US/Firefox {version}.dmg"
                ),
            }

        url = platform_urls.get(platform)
        if not url:
            raise ValueError(f"No Firefox download URL for platform: {platform}")

        self.log.debug(f"Firefox download URL for {platform}: {url}")
        return str(url)

    def _get_geckodriver_download_url(self) -> str:
        """
        🔗 Get the download URL for GeckoDriver.

        Returns:
            str: The download URL.
        """
        # Get latest release from GitHub API
        api_url = "https://api.github.com/repos/mozilla/geckodriver/releases/latest"
        response = get(api_url)
        response.raise_for_status()
        release_data = response.json()

        platform = self._get_system_platform()
        platform_mapping = {
            "win64": "win64.zip",
            "win32": "win32.zip",
            "linux64": "linux64.tar.gz",
            "mac-x64": "macos.tar.gz",
            "mac-arm64": "macos-aarch64.tar.gz",
        }

        pattern = platform_mapping.get(platform)
        if not pattern:
            raise ValueError(f"No GeckoDriver available for platform: {platform}")

        for asset in release_data["assets"]:
            if pattern in asset["name"]:
                url = str(asset["browser_download_url"])
                self.log.debug(f"GeckoDriver download URL for {platform}: {url}")
                return url

        raise ValueError(f"No GeckoDriver found for platform: {platform}")

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

    def _download_and_extract_firefox(self, url: str, version: str) -> Path:
        """
        🗃 Download and extract the Firefox executable from the given URL.

        Args:
            url (str): The URL to download Firefox from.
            version (str): The Firefox version.

        Returns:
            Path: The path to the downloaded Firefox executable.
        """
        self.log.debug(f"Downloading Firefox from {url}")
        response = get(url, allow_redirects=True)
        response.raise_for_status()

        target_dir = Path(
            self.firefox_download_dir,
            f"firefox-{self._get_system_platform()}-{version}",
        )
        target_dir.mkdir(parents=True, exist_ok=True)

        # Determine file type and extract accordingly
        if url.endswith((".zip", ".exe")):
            download_path = target_dir / "firefox.zip"
            with open(download_path, "wb") as file:
                file.write(response.content)

            if url.endswith(".exe"):
                # For .exe installers on Windows, we need to handle differently
                # This is a simplified approach - you might want to use 7zip or similar
                return download_path
            else:
                with ZipFile(download_path, "r") as zip_ref:
                    zip_ref.extractall(target_dir)
                download_path.unlink()

        elif url.endswith(".tar.bz2"):
            download_path = target_dir / "firefox.tar.bz2"
            with open(download_path, "wb") as file:
                file.write(response.content)

            with tarfile.open(download_path, "r:bz2") as tar_ref:
                tar_ref.extractall(target_dir)
            download_path.unlink()

        elif url.endswith(".dmg"):
            # For macOS DMG files, save and return path (mounting DMG is complex)
            download_path = target_dir / "firefox.dmg"
            with open(download_path, "wb") as file:
                file.write(response.content)
            return download_path

        executable_path = self._find_firefox_executable(target_dir)
        if executable_path:
            return executable_path
        raise FileNotFoundError(
            f"Could not find Firefox executable in extracted contents at {target_dir}"
        )

    def _download_and_extract_geckodriver(self, url: str) -> Path:
        """
        🗃 Download and extract the GeckoDriver executable from the given URL.

        Args:
            url (str): The URL to download GeckoDriver from.

        Returns:
            Path: The path to the downloaded GeckoDriver executable.
        """
        self.log.debug(f"Downloading GeckoDriver from {url}")
        response = get(url)
        response.raise_for_status()

        target_dir = Path(
            self.firefox_download_dir,
            f"geckodriver-{self._get_system_platform()}",
        )
        target_dir.mkdir(parents=True, exist_ok=True)

        if url.endswith(".zip"):
            download_path = target_dir / "geckodriver.zip"
            with open(download_path, "wb") as file:
                file.write(response.content)

            with ZipFile(download_path, "r") as zip_ref:
                zip_ref.extractall(target_dir)
            download_path.unlink()

        elif url.endswith(".tar.gz"):
            download_path = target_dir / "geckodriver.tar.gz"
            with open(download_path, "wb") as file:
                file.write(response.content)

            with tarfile.open(download_path, "r:gz") as tar_ref:
                tar_ref.extractall(target_dir)
            download_path.unlink()

        executable_name = "geckodriver.exe" if name == "nt" else "geckodriver"
        executable_path = target_dir / executable_name

        if executable_path.exists():
            return executable_path
        raise FileNotFoundError(
            f"Could not find GeckoDriver executable in extracted contents at {target_dir}"
        )

    @classmethod
    def cleanup_firefox_downloads(cls) -> None:
        """🧹 Clean up Firefox downloads."""
        cls.log.debug("Cleaning up Firefox downloads...")
        try:
            primary_dir = Path(config.settings.downloads_dir, ".selenium", "firefox")
            if primary_dir.exists():
                rmtree(primary_dir)
                cls.log.debug(f"Cleaned up primary directory: {primary_dir}")
        except Exception as e:
            cls.log.warning(f"Failed to clean up primary directory: {e}")

    @classmethod
    def get_download_info(cls) -> dict[str, Any]:
        """
        📊 Get information about Firefox download directories.

        Returns:
            dict: Information about download directories and their status.
        """
        primary_dir = Path(config.settings.downloads_dir, ".selenium", "firefox")
        temp_base = Path(gettempdir())

        # Find any existing temp directories
        temp_dirs = [
            d
            for d in temp_base.iterdir()
            if d.is_dir() and d.name.startswith("scriptman_firefox_")
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
