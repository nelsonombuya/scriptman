import os
import time
from enum import Enum
from typing import Optional, Union

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from scriptman import Settings
from scriptman.directories import DirectoryHandler

AnyDriver = Union[webdriver.Chrome, webdriver.Firefox]


class Interaction(Enum):
    """
    Interaction defines the possible interaction modes when interacting with
    web elements.

    Attributes:
        CLICK (str): Click on the web element.
        JS_CLICK (str): Perform a JavaScript click on the web element.
        SEND_KEYS (str): Send keys (text input) to the web element.
        WAIT_TILL_INVISIBLE (str): Wait for the element to become invisible.
        DENY_COOKIES (str): Deny cookies using JavaScript interaction.
    """

    CLICK = "click"
    JS_CLICK = "js_click"
    SEND_KEYS = "send_keys"
    WAIT_TILL_INVISIBLE = "wait"
    DENY_COOKIES = "deny_cookies"


class SeleniumInteractionHandler:
    """
    SeleniumInteractionHandler provides methods for interacting with web
    elements using Selenium WebDriver.

    Attributes:
        _driver (AnyDriver): The Selenium WebDriver instance
            (Chrome or Firefox).
        _downloads_directory (str): The directory path for downloads.
    """

    def __init__(self, driver: AnyDriver) -> None:
        """
        Initialize the SeleniumInteractionHandler instance with the provided
        WebDriver.

        Args:
            driver (AnyDriver): The Selenium WebDriver instance
                (Chrome or Firefox).
        """
        self._driver = driver
        self._downloads_directory = DirectoryHandler().downloads_dir

    def interact_with_element(
        self,
        xpath: str,
        mode: Interaction = Interaction.CLICK,
        keys: Optional[str] = None,
        timeout: int = 30,
        rest: float = 0.5,
    ) -> None:
        """
        Interact with a web element on the page.

        This method interacts with a web element identified by the provided
        XPath on the web page. The interaction can include clicking the
        element, sending keys (text input) to it, waiting for it to become
        invisible, or denying cookies using JavaScript.

        Args:
            xpath (str): The XPath expression to locate the web element.
            mode (Interaction, optional): The interaction mode, which can be
                one of the Interaction enum values.
                (default is Interaction.CLICK)
            keys (str, optional): The text to send to the element when using
                Interaction.SEND_KEYS mode. Ignored if mode is not
                Interaction.SEND_KEYS.
            timeout (int, optional): The maximum time (in seconds) to wait for
                the element to become clickable or invisible. Default is 30.
            rest (float, optional): The time (in seconds) to rest after the
                interaction. Default is 0.5.

        Raises:
            ValueError: If an invalid interaction mode is provided.
        """
        wait = WebDriverWait(self._driver, timeout)
        if mode == Interaction.WAIT_TILL_INVISIBLE:
            wait.until(EC.invisibility_of_element_located((By.XPATH, xpath)))
            return

        element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        ActionChains(self._driver).move_to_element(element).perform()

        if mode == Interaction.CLICK:
            element.click()
        elif mode == Interaction.JS_CLICK:
            self._driver.execute_script("arguments[0].click();", element)
        elif mode == Interaction.SEND_KEYS:
            element.send_keys(keys)
        elif mode == Interaction.DENY_COOKIES:
            return self.interact_with_element(
                mode=Interaction.JS_CLICK,
                xpath='//*[@id="tarteaucitronAllDenied2"]',
            )
        else:
            raise ValueError(f"Passed Invalid Mode: {mode}")
        time.sleep(2 if Settings.debug_mode else rest)

    def wait_for_downloads_to_finish(self) -> None:
        """
        Wait for all downloads to finish before continuing.
        """
        directory = self._downloads_directory
        files = os.listdir(directory)

        def is_new_file_added(self) -> bool:
            current_files = os.listdir(directory)
            new_files = [
                file_name
                for file_name in current_files
                if file_name not in files
                and not file_name.endswith((".tmp", ".crdownload"))
            ]
            return len(new_files) > 0

        WebDriverWait(self._driver, 300, 1).until(is_new_file_added)
