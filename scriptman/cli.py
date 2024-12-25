import sys
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from loguru import logger

from config import settings


class CLIHandler:
    """
    🤖 Comprehensive Command Line Interface Handler for managing and running scripts.

    This class provides a flexible, platform-agnostic approach to script management
    with extensive configuration options.
    """

    def __init__(self) -> None:
        """🚀 Initialize the CLIHandler."""
        self._initialize_logging()

    def _initialize_logging(self) -> None:
        """
        📝 Initialize logging for the CLIHandler.

        Configure the loguru logger to output to a file at
        `scriptman/logs/{current_timestamp}.log` in the current directory.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file = Path(__file__).resolve().parent / "logs" / f"{timestamp}.log"
        file.parent.mkdir(parents=True, exist_ok=True)

        logger.add(
            file,
            level="DEBUG",
            rotation="1 day",
            compression="zip",
            retention="30 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        )
        logger.info("✅ Logging initialized")

    def _create_parser(self) -> ArgumentParser:
        """
        🏪 Create an argparse parser with all supported CLI options.

        Returns:
            ArgumentParser: Configured argument parser.
        """
        parser = ArgumentParser(
            description="ScriptMan: Flexible Script Management Tool",
            epilog="Run scripts with advanced configuration options.",
        )

        # Configuration flags
        parser.add_argument(
            "-d",
            "--debug",
            action="store_true",
            help="Enable debugging mode",
        )
        parser.add_argument(
            "-q",
            "--quick",
            action="store_true",
            help="Enable quick mode (skip updates and installation)",
        )
        parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="Force scripts to run even if another instance is running",
        )
        parser.add_argument(
            "-c",
            "--custom",
            action="store_true",
            help="Enable custom mode (specify custom script files using their path)",
        )

        # Script selection
        parser.add_argument(
            "-i",
            "--ignore",
            nargs="*",
            metavar="SCRIPT",
            help="Specify scripts to ignore",
        )
        parser.add_argument(
            "scripts",
            nargs="*",
            help="Specific scripts to run (optional)",
        )

        return parser

    def run(self, argv: Optional[List[str]] = None) -> int:
        """
        Parse arguments and execute scripts based on CLI configuration.

        Args:
            argv (Optional[List[str]]): Command-line arguments.
                Uses sys.argv[1:] if not provided.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """
        # Create parser and parse arguments
        parser = self._create_parser()
        args = parser.parse_args(argv or sys.argv[1:])

        # Apply global settings based on flags
        if args.debug:
            settings.debug = True

        logger.info("🚀 Starting ScriptMan...")
        logger.debug(f"Arguments used: {args}")
        return 0

    @staticmethod
    def start_cli_instance(argv: Optional[List[str]] = None) -> int:
        """
        🌟 Start a new instance of the CLIHandler and execute scripts.

        Args:
            argv (Optional[List[str]]): Command-line arguments to pass to the CLI.
                If not provided, defaults to sys.argv[1:].

        Returns:
            int: Exit code from the CLIHandler execution (0 for success, non-zero for
                failure).
        """

        return CLIHandler().run(argv)
