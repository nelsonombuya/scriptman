"""
ScriptMan - SeleniumHandler and SeleniumInteractionHandler

This module provides the SeleniumHandler and SeleniumInteractionHandler classes
for managing Selenium WebDriver instances and interacting with web elements.

Usage:
- Import the SeleniumHandler and SeleniumInteractionHandler classes from this
module.
- Initialize a SeleniumHandler instance to manage Selenium WebDriver instances.
- Use the SeleniumInteractionHandler to interact with web elements using a
WebDriver instance.

Example:
```python
from scriptman._selenium import (
    SeleniumHandler,
    SeleniumInteractionHandler,
    SeleniumInteraction
)

# Initialize a SeleniumHandler instance
selenium_handler = SeleniumHandler()

# Create a Chrome WebDriver instance with default settings
driver = selenium_handler.chrome.get_driver()

# Initialize a SeleniumInteractionHandler instance with the WebDriver
interaction_handler = SeleniumInteractionHandler(driver)

# Interact with a web element using various modes
interaction_handler.interact_with_element(
    xpath="//button[@id='submit']",
    mode=SeleniumInteraction.CLICK
)
interaction_handler.interact_with_element(
    xpath="//input[@id='search']",
    mode=SeleniumInteraction.SEND_KEYS,
    keys="search_text"
)

# Wait for downloads to finish
interaction_handler.wait_for_downloads_to_finish()
```

Classes:
- `SeleniumHandler`: Manages the creation of Selenium WebDriver instances.
- `SeleniumInteractionHandler`: Provides methods for interacting with web
elements using Selenium WebDriver.

Attributes:
- None

Methods (SeleniumHandler):
- `chrome`: A callable attribute that returns an instance of the `Chrome` class
for creating Chrome WebDriver instances.

Methods (SeleniumInteractionHandler):
- `__init__(self, driver: AnyDriver) -> None`: Initializes a
SeleniumInteractionHandler instance with the provided WebDriver.
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

See Also:
- `Chrome`: The class responsible for configuring and creating Chrome WebDriver
instances.
- `SeleniumInteraction`: An enum that defines possible interaction modes when
interacting with web elements.
"""

import os
import time
from enum import Enum
from typing import Callable, Optional, Union

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from scriptman._chrome import Chrome
from scriptman._directories import DirectoryHandler
from scriptman._settings import Settings

AnyDriver = Union[webdriver.Chrome, webdriver.Firefox]


class SeleniumHandler:
    def __init__(self) -> None:
        """
        SeleniumHandler manages the creation of Selenium WebDriver instances.

        SeleniumHandler is responsible for creating and managing instances of
        Selenium WebDriver for various browsers.

        > Chrome
        It utilizes the `Chrome` class to create WebDriver instances with
        various configurations, allowing users to automate web interactions
        using Selenium.

        Example:
            To create a Selenium WebDriver instance with default settings:
            >>> selenium_handler = SeleniumHandler()
            >>> driver = selenium_handler.chrome.get_driver()

            You can also use the browser instance for extra functionality:
            >>> selenium_handler = SeleniumHandler.chrome
            >>> selenium_handler.wait_for_downloads_to_finish()
            > See `SeleniumInteractionsHandler` for more details.
        """

    chrome: Callable[[], Chrome] = Chrome


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
        SeleniumInteractionHandler provides methods for interacting with web
        elements using Selenium WebDriver.

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
        if mode == SeleniumInteraction.DENY_COOKIES:
            return self.interact_with_element(
                mode=SeleniumInteraction.JS_CLICK,
                xpath=xpath or '//*[@id="tarteaucitronAllDenied2"]',
            )

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
