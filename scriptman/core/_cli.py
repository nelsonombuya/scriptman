import sys
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from loguru import logger

from scriptman.core._config import config_handler


class CLIHandler:
    """
    🤖 Comprehensive Command Line Interface Handler for managing and running scripts.

    This class provides a flexible, platform-agnostic approach to script management with
    extensive configuration options.
    """

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

        """
        Install

        Usage: scriptman install [-b] [-p]

        Used to install the scriptman package using poetry (update dependencies).
        Optionally, build and publish the package.
        """
        subparsers = parser.add_subparsers(dest="action", help="Available actions")
        install_parser = subparsers.add_parser("package", help="Build Scriptman package")
        install_parser.add_argument(
            "-u",
            "--update",
            action="store_true",
            help="Update scriptman dependencies and version based on current commit.",
        )
        install_parser.add_argument(
            "-b",
            "--build",
            action="store_true",
            help="Set this flag to enable build during install.",
        )
        install_parser.add_argument(
            "-p",
            "--publish",
            action="store_true",
            help="Set this flag to enable publish during install.",
        )

        """
        Config

        Usage: scriptman config CONFIG VALUE

        Used to update configuration options.
        """
        config_parser = subparsers.add_parser("config", help="Update configuration")
        config_parser.add_argument(
            "config",
            choices=config_handler.configs.keys(),
            help="Set configuration parameter to a specified value. "
            "Available options: "
            + ", ".join(
                f"{k} ({v["type"]},  {v["description"]})"
                for k, v in config_handler.configs.items()
            ),
        )
        config_parser.add_argument(
            "value",
            help="The value to set for the specified configuration parameter.",
        )

        """
        Run

        Usage: scriptman run [-s SCRIPTS]

        Used to run scripts with optional script selection.
        """
        run_parser = subparsers.add_parser("run", help="Run scripts")
        run_parser.add_argument(
            "-s",
            "--scripts",
            nargs="*",
            metavar="SCRIPT",
            help="Specify scripts to run. "
            "If not defined, will run all python scripts in the working directory. "
            'Example: -s script1.py script2.py "another script.py" "path/to/script.py"',
        )
        run_parser.add_argument(
            "-r",
            "--retries",
            type=int,
            default=0,
            help="Specify the number of times to retry running the scripts in case of "
            "failure. Default is 0 (no retries).",
        )

        """
        Version

        Usage: scriptman --version

        Display the current version of the Scriptman package.
        """
        parser.add_argument(
            "-v",
            "--version",
            action="version",
            help="Display version",
            version=f"Scriptman {config_handler.version}",
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
        parser = self._create_parser()
        args = parser.parse_args(argv or sys.argv[1:])

        # Initialize the application
        self._initialize_logging()

        if args.action == "package":
            return config_handler._manage_scriptman_package(
                publish=args.publish,
                update=args.update,
                build=args.build,
            )

        if args.action == "config":
            return config_handler._update_configuration(
                param=args.config,
                value=args.value,
            )

        if args.action == "run":
            from scriptman.core._scripts import ScriptsHandler

            logger.info("🚀 Starting ScriptMan...")
            scripts = [Path(_) for _ in args.scripts] or list(Path(".").glob("*.py"))

            if not scripts:
                logger.error("❓ No scripts found in the current directory.")
                return 1

            if args.retries < 0:
                logger.error("❌ Invalid number of retries provided. Must be >= 0.")
                return 1

            if args.retries >= 0:
                config_handler.config.retries = args.retries

            ScriptsHandler().run_scripts(scripts)

        return 0

    def _initialize_logging(self, verbose: bool = False) -> None:
        """
        📝 Initialize logging for the CLIHandler.

        Configure the loguru logger to output to a file at
        `scriptman/logs/{current_timestamp}.log` in the current directory.

        Args:
            verbose (bool): Enable verbose logging (default: False)
        """
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file = Path(config_handler.config.logs_dir) / f"{timestamp}.log"

        # TODO: Implement consistent console and file logging without line information
        logger.add(
            file,
            colorize=True,
            rotation="1 day",
            compression="zip",
            retention="30 days",
            level="DEBUG" if verbose else "INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
        )
        logger.debug(f"✅ Logging initialized to {file}")
