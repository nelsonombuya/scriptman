"""
ScriptMan - LogHandler

This module provides the LogHandler class for handling logging operations.

Usage:
- Import the LogHandler class from this module.
- Initialize a LogHandler instance to configure and use custom logging.

Example:
```python
from scriptman._logs import LogHandler

log = LogHandler(name="MyLog", filename="my_log", module="MyModule")
log.message("This is an information message.", level=LogLevel.INFO)
```

Classes:
- `LogHandler`: Handles logging operations and provides customization options.

Enums:
- `LogLevel`: Defines log levels for messages.

Attributes:
- None

Methods:
- `__init__(
        self,
        name: str = "LOG",
        filename: str = "LOG",
        module: Optional[str] = None,
        level: LogLevel = LogLevel.INFO,
        description: Optional[str] = None
    ) -> None`: Initializes a LogHandler instance.
- `_format_name(self,
        name: str,
        module: Optional[str] = None
    ) -> str`: Formats the log name with optional module.
- `_get_log_file(
        self,
        filename: str
    ) -> Optional[str]`: Generates a log file path.
- `_configure_logging(self) -> None`: Configures logging settings.
- `_get_log_level(
        self,
        level: LogLevel
    ) -> int`: Converts LogLevel enum to Python logging level.
- `start(self) -> None`: Records the start time of an operation and logs a
    start message.
- `stop(self) -> None`: Stops an operation, records the end time, and logs a
    finish message.
- `message(
        self,
        message: str,
        level: LogLevel = LogLevel.INFO,
        details: Optional[Dict[str, Any]] = None,
        print_to_terminal: bool = True
    ) -> None`: Logs a message with optional details.
- `format_time(
        self,
        seconds: int
    ) -> str`: Formats seconds as an Hour, Minute, Second formatted string.

"""

import logging
import time
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from scriptman._directories import DirectoryHandler
from scriptman._settings import Settings


class LogLevel(Enum):
    INFO = "INFO"
    WARN = "WARN"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    FATAL = "FATAL"
    CRITICAL = "CRITICAL"
    EXCEPTION = "EXCEPTION"


class LogHandler:
    def __init__(
        self,
        name: str = "LOG",
        filename: str = "LOG",
        module: Optional[str] = None,
        level: LogLevel = LogLevel.INFO,
        description: Optional[str] = None,
    ) -> None:
        """
        Initializes a LogHandler instance.

        Args:
            name (str): The name for the log.
            filename (str): The base filename for the log file.
            module (str, optional): The module name for the log.
                Defaults to None.
            level (LogLevel, optional): The logging level.
                Defaults to LogLevel.INFO.
            description (str, optional): The description for the log.
                Defaults to None.
        """
        self._level_map = {
            LogLevel.WARN: logging.WARN,
            LogLevel.INFO: logging.INFO,
            LogLevel.ERROR: logging.ERROR,
            LogLevel.FATAL: logging.FATAL,
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.CRITICAL: logging.CRITICAL,
        }

        self._name = name.upper().replace(" ", "_")
        self._module = module.upper() if module else None
        self._title = name.title().replace("_", " ")
        self._logs_dir = DirectoryHandler().logs_dir
        self._description = description or self._title
        self._file = self._get_log_file(filename)
        self._configure_logging(level)

    def _get_log_file(self, filename: str) -> Optional[str]:
        """
        Generates a log file path.

        Args:
            filename (str): The base filename for the log file.

        Return:
            (str | None): The file path of the log file, or None if the
                Settings.log_mode flag is False.
        """
        if not Settings.log_mode:
            return None
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return rf"{self._logs_dir}\{filename} - {timestamp}.log"

    def _configure_logging(self, level: LogLevel) -> None:
        """
        Configures logging to a file.
        """
        logging.basicConfig(
            filename=self._file,
            level=self._get_log_level(level),
            format="%(asctime)s %(levelname)s:%(message)s",
        )

    def _get_log_level(self, level: LogLevel) -> int:
        """
        Get's the logging level according to the specified LogLevel.

        Args:
            level (LogLevel): The LogLevel Enum to be set.

        Return:
            (int): The logging level to be used by the rest of the class.
                logging.DEBUG if the Settings.debug_mode flag is True, else
                logging.INFO if the specified level is not found.
        """
        return self._level_map.get(
            level, logging.DEBUG if Settings.debug_mode else logging.INFO
        )

    def start(self) -> None:
        """
        Record the start time of an operation and log a start message.
        """
        self._start_time = time.time()
        self.message(f"{self._title} started.")

    def stop(self) -> None:
        """
        Stop and record the end time of an operation, log an end message, and
        calculate the duration.
        """
        self._end_time = time.time()
        time_taken = self.format_time(int(self._end_time - self._start_time))
        self.message(f"{self._title} finished in {time_taken}")

    def message(
        self,
        message: str,
        level: LogLevel = LogLevel.INFO,
        details: Optional[Dict[str, Any]] = None,
        print_to_terminal: bool = True,
    ) -> None:
        """
        Log a message with optional details and print it to the terminal.

        Args:
            message (str): The main message to log.
            level (LogLevel): The log level for the message.
            details (Dict[str, Any]): Optional details to include in the log
                message.
            print_to_terminal (bool): Whether to print the message to the
                terminal.
        """
        formatted_message = f"[{self._name}] {message}"
        formatted_message += (
            "\n\t" + ("\n\t".join([f"{k}: {v}" for k, v in details.items()]))
            if details
            else ""
        )

        if Settings.log_mode:
            logging.log(self._get_log_level(level), formatted_message)

        if print_to_terminal and Settings.print_logs_to_terminal:
            timestamp = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            print(f"{timestamp} [{level.value}] {formatted_message}")

    def info(self, message: str) -> None:
        """
        Log an information message.

        Args:
            message (str): The main message to log.
        """
        self.message(message, LogLevel.INFO)

    def warn(self, message: str) -> None:
        """
        Log a warning message.

        Args:
            message (str): The main message to log.
        """
        self.message(message, LogLevel.WARN)

    def debug(self, message: str) -> None:
        """
        Log a debug message.

        Args:
            message (str): The main message to log.
        """
        self.message(message, LogLevel.DEBUG)

    def error(self, message: str) -> None:
        """
        Log an error message.

        Args:
            message (str): The main message to log.
        """
        self.message(message, LogLevel.ERROR)

    def fatal(self, message: str) -> None:
        """
        Log a fatal message.

        Args:
            message (str): The main message to log.
        """
        self.message(message, LogLevel.FATAL)

    def critical(self, message: str) -> None:
        """
        Log a critical message.

        Args:
            message (str): The main message to log.
        """
        self.message(message, LogLevel.CRITICAL)

    def exception(self, message: str, exception: Exception) -> None:
        """
        Log an exception message with the exception details.

        Args:
            message (str): The main message to log.
            exception (Exception): The exception to log.
        """
        self.message(message, LogLevel.EXCEPTION, {"Exception": exception})

    @staticmethod
    def format_time(seconds: int) -> str:
        """
        Returns the seconds as an Hour, Minute, Second formatted string.

        Args:
            seconds (int): The number of seconds to format.

        Returns:
            str: The formatted time.
        """
        if seconds == 0:
            return "0 Seconds"

        hrs = seconds // 3600
        mins = (seconds % 3600) // 60
        rem_secs = seconds % 60

        formatted_time = ""

        if hrs > 0:
            formatted_time += f"{hrs} {'Hour' if hrs == 1 else 'Hours'}"

        if mins > 0:
            if formatted_time:
                formatted_time += " "
            formatted_time += f"{mins} {'Minute' if mins == 1 else 'Minutes'}"

        if rem_secs > 0:
            if formatted_time:
                formatted_time += " "
            string = f"{rem_secs} {'Second' if rem_secs == 1 else 'Seconds'}"
            formatted_time += string

        return f"{formatted_time}."

    def __str__(self) -> str:
        return f"{self._name} - {self._description}"
