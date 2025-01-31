from abc import ABC
from pathlib import Path
from random import uniform
from time import sleep
from typing import Literal, Optional

from loguru import logger

from scriptman.core.config import config
from scriptman.utils.selenium._enums import SeleniumBrowser

try:
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.wait import WebDriverWait

    from scriptman.utils.selenium._enums import BrowserMap, Browsers, Driver
    from scriptman.utils.selenium._chrome import Chrome
except ImportError:
    raise ImportError(
        "Selenium is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[selenium]."
    )


class SeleniumInstance(ABC):
    def __init__(
        self,
        browser: Browsers = Browsers.CHROME_LOCAL,
        browser_queue: Optional[list[Browsers]] = None,
    ) -> None:
        """
        🚀 Initialize SeleniumInstance with the given browser and optional browser queue.

        Args:
            browser (Browsers, optional): The browser to use. Defaults to
                Browsers.CHROME_LOCAL.
            browser_queue (Optional[list[Browsers]], optional): The browser queue to use
                for the instance, such that if one fails, it will try the next one.
                Defaults to None.
        """
        self._queue: Optional[list[Browsers]] = browser_queue
        self._browser: SeleniumBrowser = BrowserMap.get(browser, Chrome)()

        if browser in [Browsers.CHROME_LOCAL] and isinstance(self._browser, Chrome):
            self._browser._local_mode = True

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
        mode: Literal[
            "wait",
            "click",
            "js_click",
            "send_keys",
            "send_return",
            "deny_cookies",
            "accept_cookies",
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
            mode (Literal, optional): The interaction mode. Defaults to "click".

        Returns:
            bool: True if the interaction was successful, False otherwise.
        """
        logger.debug(
            f"Interacting with element: {xpath} (mode: {mode}) "
            f"Timeout: {timeout} "
            f"Rest: {rest}."
        )

        sleep(rest)  # Rest before each interaction to offset the bot detection
        if mode in ["deny_cookies", "accept_cookies"]:  # Deny or accept cookies
            mode = "js_click"
            xpath = xpath or (
                '//*[@id="tarteaucitronAllDenied2"]'
                if mode == "deny_cookies"
                else '//*[@id="tarteaucitronAllAllowed2"]'
            )

        wait = WebDriverWait(self.driver, timeout)
        if mode == "wait":  # Wait for the element to become invisible
            wait.until(EC.invisibility_of_element_located((By.XPATH, xpath)))
            return True

        element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        ActionChains(self.driver).move_to_element(element).perform()

        if mode == "click":  # Click on the web element
            element.click()
            return True

        if mode == "js_click":  # Perform a JavaScript click on the web element
            self.driver.execute_script("arguments[0].click();", element)
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

    def wait_for_downloads_to_finish(self, file_name: Optional[str] = None) -> None:
        """
        ⌚ Wait for all downloads to finish before continuing.

        Args:
            file_name (Optional[str]): The name of the file you want to wait for its
                download to complete. Defaults to None.
        """
        directory = Path(config.env.downloads_dir)
        files = [f.name for f in directory.iterdir()]
        download_extensions = (".tmp", ".crdownload")

        if not file_name:

            def is_new_file_added(driver) -> bool:
                current_files = list(directory.iterdir())
                new_files = [
                    file
                    for file in current_files
                    if file not in files and file.suffix not in download_extensions
                ]
                return len(new_files) > 0

            WebDriverWait(self.driver, 300, 1).until(is_new_file_added)
            return
        else:

            def does_file_exist(driver) -> bool:
                return bool(list(Path(directory).glob(f"{file_name}*")))

            WebDriverWait(self.driver, 300, 1).until(does_file_exist)

    def __del__(self) -> None:
        """
        🧹 Close the WebDriver instance when the InteractionHandler instance is deleted.
        """
        self.driver.quit()
