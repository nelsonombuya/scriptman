"""
ScriptMan - SeleniumInteractionHandler

This module provides the SeleniumInteractionHandler class for interacting with
web elements using Selenium WebDriver.

Usage:
- Import the SeleniumInteractionHandler class from this module.
- Initialize an instance of SeleniumInteractionHandler with a WebDriver
instance.
- Use the provided methods to interact with web elements on a web page.

Class:
- `SeleniumInteractionHandler`: Provides methods for interacting with web
elements using Selenium WebDriver.

Attributes:
- None

Methods:
- `__init__(self, driver: AnyDriver) -> None`: Initialize the
SeleniumInteractionHandler instance.
- `interact_with_element(
    self,
    xpath: str,
    mode: SeleniumInteraction = SeleniumInteraction.CLICK,
    keys: Optional[str] = None,
    timeout: int = 30,
    rest: float = 0.5
) -> None`: Interact with a web element on the page.
- `wait_for_downloads_to_finish(self) -> None`: Wait for all downloads to
finish before continuing.

Enum:
- `SeleniumInteraction`: Defines possible interaction modes when interacting
with web elements.

Attributes:
    - `CLICK (str)`: Click on the web element.
    - `JS_CLICK (str)`: Perform a JavaScript click on the web element.
    - `SEND_KEYS (str)`: Send keys (text input) to the web element.
    - `WAIT_TILL_INVISIBLE (str)`: Wait for the element to become invisible.
    - `DENY_COOKIES (str)`: Deny cookies using JavaScript interaction.
    - `ACCEPT_COOKIES (str)`: Accept cookies using JavaScript interaction.

Initialization:
- Create an instance of `SeleniumInteractionHandler` by providing a WebDriver
instance (Chrome or Firefox) to interact with web elements.

Methods:
- `interact_with_element(
    self,
    xpath: str,
    mode: SeleniumInteraction = SeleniumInteraction.CLICK,
    keys: Optional[str] = None,
    timeout: int = 30,
    rest: float = 0.5
    ) -> None`: Interact with a web element on the page.
    - `xpath (str)`: The XPath expression to locate the web element.
    - `mode (SeleniumInteraction)`: The interaction mode (default is CLICK).
    - `keys (str, optional)`: The text to send to the element when using
        SEND_KEYS mode (ignored if mode is not SEND_KEYS).
    - `timeout (int, optional)`: The maximum time (in seconds) to wait for the
        element to become clickable or invisible (default is 30).
    - `rest (float, optional)`: The time (in seconds) to rest after the
        interaction (default is 0.5).
- `wait_for_downloads_to_finish(self) -> None`: Wait for all downloads to
    finish before continuing.

Raises:
- `ValueError`: If an invalid interaction mode is provided.

Examples:
```python
# Initialize SeleniumInteractionHandler
driver = webdriver.Chrome()
interaction_handler = SeleniumInteractionHandler(driver)

# Interact with a web element
interaction_handler.interact_with_element(
    "//button[@id='example']",
    mode=SeleniumInteraction.CLICK
)
```

Enum:
- `SeleniumInteraction`: Defines possible interaction modes when interacting
    with web elements.
    - `CLICK (str)`: Click on the web element.
    - `JS_CLICK (str)`: Perform a JavaScript click on the web element.
    - `SEND_KEYS (str)`: Send keys (text input) to the web element.
    - `WAIT_TILL_INVISIBLE (str)`: Wait for the element to become invisible.
    - `DENY_COOKIES (str)`: Deny cookies using JavaScript interaction.
    - `ACCEPT_COOKIES (str)`: Accept cookies using JavaScript interaction.
"""

import os
import time
from enum import Enum
from random import uniform
from typing import Optional, Union

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from scriptman._directories import DirectoryHandler
from scriptman._settings import Settings

AnyDriver = Union[webdriver.Chrome, webdriver.Edge, webdriver.Firefox]


class SeleniumInteraction(Enum):
    """
    SeleniumInteraction defines the possible interaction modes when
    interacting with web elements.

    Attributes:
        CLICK (str): Click on the web element.
        JS_CLICK (str): Perform a JavaScript click on the web element.
        SEND_KEYS (str): Send keys (text input) to the web element.
        WAIT_TILL_INVISIBLE (str): Wait for the element to become invisible.
        DENY_COOKIES (str): Deny cookies using JavaScript interaction.
        ACCEPT_COOKIES (str): Accept cookies using JavaScript interaction.
    """

    CLICK = "click"
    JS_CLICK = "js_click"
    SEND_KEYS = "send_keys"
    WAIT_TILL_INVISIBLE = "wait"
    DENY_COOKIES = "deny_cookies"
    ACCEPT_COOKIES = "accept_cookies"


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
        mode: SeleniumInteraction = SeleniumInteraction.CLICK,
        keys: Optional[str] = None,
        timeout: int = 30,
        rest: float = uniform(0.25, 0.50),
    ) -> None:
        """
        Interact with a web element on the page.

        This method interacts with a web element identified by the provided
        XPath on the web page. The interaction can include clicking the
        element, sending keys (text input) to it, waiting for it to become
        invisible, or denying cookies using JavaScript.

        Args:
            xpath (str): The XPath expression to locate the web element.
                You can input an empty xpath ("") if mode is
                SeleniumInteraction.DENY_COOKIES or
                SeleniumInteraction.ACCEPT_COOKIES.
            mode (SeleniumInteraction, optional): The interaction mode, which
                can be one of the Interaction enum values.
                (default is SeleniumInteraction.CLICK).
            keys (str, optional): The text to send to the element when using
                SeleniumInteraction.SEND_KEYS mode. Ignored if mode is not
                SeleniumInteraction.SEND_KEYS.
            timeout (int, optional): The maximum time (in seconds) to wait for
                the element to become clickable or invisible (default is 30).
            rest (float, optional): The time (in seconds) to rest after the
                interaction (default is a random time between 0.25s and 0.50s).

        Raises:
            ValueError: If an invalid interaction mode is provided.
        """
        if mode in [
            SeleniumInteraction.DENY_COOKIES,
            SeleniumInteraction.ACCEPT_COOKIES,
        ]:
            xpath = xpath or (
                '//*[@id="tarteaucitronAllDenied2"]'
                if mode == SeleniumInteraction.DENY_COOKIES
                else '//*[@id="tarteaucitronAllAllowed2"]'
            )
            mode = SeleniumInteraction.JS_CLICK

        wait = WebDriverWait(self._driver, timeout)
        if mode == SeleniumInteraction.WAIT_TILL_INVISIBLE:
            wait.until(EC.invisibility_of_element_located((By.XPATH, xpath)))
            return

        element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        ActionChains(self._driver).move_to_element(element).perform()

        if mode == SeleniumInteraction.CLICK:
            element.click()
        elif mode == SeleniumInteraction.JS_CLICK:
            self._driver.execute_script("arguments[0].click();", element)
        elif mode == SeleniumInteraction.SEND_KEYS:
            element.send_keys(keys)
        else:
            raise ValueError(f"Invalid mode: {mode}")
        time.sleep(1 if Settings.debug_mode else rest)

    def wait_for_downloads_to_finish(
        self,
        file_name: Optional[str] = None,
    ) -> None:
        """
        Wait for all downloads to finish before continuing.

        Args:
            file_name (optional(str)): The name of the file you want to wait
                for its download to complete. Defaults to None.
        """
        download_extensions = (".tmp", ".crdownload")
        directory = self._downloads_directory
        files = os.listdir(directory)

        def is_new_file_added(self) -> bool:
            current_files = os.listdir(directory)
            new_files = [
                file
                for file in current_files
                if file not in files and not file.endswith(download_extensions)
            ]
            return len(new_files) > 0

        def does_file_exist(self) -> bool:
            return file_name in os.listdir(directory)

        if file_name:
            WebDriverWait(self._driver, 300, 1).until(does_file_exist)
        else:
            WebDriverWait(self._driver, 300, 1).until(is_new_file_added)

    def __del__(self) -> None:
        """
        Close the WebDriver instance when the SeleniumInteractionHandler
        instance is deleted.
        """
        self._driver.quit()
