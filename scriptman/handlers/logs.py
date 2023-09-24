import logging
import time
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from .directories import DirectoryHandler
from .settings import settings


class LogLevel(Enum):
    INFO = "INFO"
    WARN = "WARN"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    FATAL = "FATAL"
    CRITICAL = "CRITICAL"
    EXCEPTION = "EXCEPTION"


class LogHandler:
    """
    LogHandler handles logging operations, providing flexibility and control
    over logging configuration.
    """

    def __init__(
        self,
        name: str = "LOG",
        filename: str = "LOG",
        level: LogLevel = LogLevel.INFO,
        description: Optional[str] = None,
    ) -> None:
        self.level: LogLevel = level
        self.name: str = name.upper().replace(" ", "_")
        self.title: str = name.title().replace("_", " ")
        self.description: str = description or self.title
        self.file: Optional[str] = self._get_log_file(filename)
        self._configure_logging()

    def _get_log_file(self, filename: str) -> Optional[str]:
        if not settings.log_mode:
            return None
        directory = DirectoryHandler().logs_dir
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return rf"{directory}\{filename} - {timestamp}.log"

    def _configure_logging(self) -> None:
        logging.basicConfig(
            filename=self.file,
            level=self._get_log_level(self.level),
            format="%(asctime)s %(levelname)s:%(message)s",
        )

    def _get_log_level(self, level: LogLevel) -> int:
        return {
            LogLevel.WARN: logging.WARN,
            LogLevel.INFO: logging.INFO,
            LogLevel.ERROR: logging.ERROR,
            LogLevel.FATAL: logging.FATAL,
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.CRITICAL: logging.CRITICAL,
        }.get(level, logging.DEBUG if settings.debug_mode else logging.INFO)

    def start(self) -> None:
        """
        Record the start time of an operation and log a start message.
        """
        self.start_time = time.time()
        section = self.description.replace("_", " ")
        self.message(f"{section.title()} started.")

    def stop(self) -> None:
        """
        Stop and record the end time of an operation, log an end message, and
        calculate the duration.
        """
        self.end_time = time.time()
        section = self.description.replace("_", " ")
        time_taken = self.format_time(int(self.end_time - self.start_time))
        self.message(f"{section.title()} finished in {time_taken}")

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
        message = f"{self.name}: {message}"
        message += (
            "\n\t" + ("\n\t".join([f"{k}: {v}" for k, v in details.items()]))
            if details
            else ""
        )

        if settings.log_mode:
            {
                LogLevel.INFO: logging.info,
                LogLevel.DEBUG: logging.debug,
                LogLevel.ERROR: logging.error,
                LogLevel.FATAL: logging.fatal,
                LogLevel.WARN: logging.warning,
                LogLevel.CRITICAL: logging.critical,
                LogLevel.EXCEPTION: logging.exception,
            }.get(level, logging.info)(message)

        if print_to_terminal:
            print(message)

    def format_time(self, seconds: int) -> str:
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
