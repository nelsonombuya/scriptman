from argparse import ArgumentParser, Namespace

from scriptman.core.cli._parser import BaseParser
from scriptman.core.config import config
from scriptman.utils.cleanup import CleanUp


class GeneralParser(BaseParser):

    def __init__(self, parser: ArgumentParser) -> None:
        """
        🚀 Initializes a GeneralParser instance with an ArgumentParser.

        Args:
            parser: ArgumentParser instance to use for parsing CLI arguments.
        """
        self.parser: ArgumentParser = parser

        # Initialize sub-commands
        self.general_arguments()

    @property
    def command(self) -> str:
        """
        ⚙ Get the name of the command being parsed.

        Returns:
            str: The name of the command being parsed.
        """
        return "general"

    def general_arguments(self):
        """
        ⚙ Adds general arguments to the CLI parser.

        This includes the following arguments:

        - `-r` or `--reset`: Reset configuration to default values, and delete
            `scriptman.toml` file.
        - `-c` or `--cleanup`: Clean up cache, downloaded files, and logs older than
            30 days.
        - `-v` or `--version`: Display version.
        """
        self.parser.add_argument(
            "-r",
            "--reset",
            action="store_true",
            help="Reset configuration to default values, and delete scriptman.toml file",
        )
        self.parser.add_argument(
            "-c",
            "--cleanup",
            action="store_true",
            help="Clean up cache, downloaded files, and logs older than 30 days.",
        )
        self.parser.add_argument(
            "-v",
            "--version",
            action="version",
            help="Display version",
            version=f"Scriptman {config._version}",
        )
        self.parser.add_argument(
            "-u",
            "--update",
            nargs="?",
            const="latest",
            metavar="VERSION",
            help="Update Scriptman Package to the given version "
            "(or 'latest' if not provided). "
            "NOTE: Kindly use semantic versioning for the version.",
        )

    def process(self, args: Namespace) -> int:
        """
        ⚙ Process parsed CLI arguments for the 'general' command.

        This function takes the parsed CLI arguments as a Namespace object and performs
        actions based on the provided options.

        Args:
            args (Namespace): Parsed CLI arguments containing the following attributes:

                - reset (bool): If True, reset configuration to default values and delete
                the scriptman.toml file.
                - cleanup (bool): If True, clean up cache, downloaded files, and logs
                older than 30 days.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """

        if args.reset:
            config.reset_all_configurations()
            return 0

        if args.cleanup:
            CleanUp().cleanup()
            return 0

        if args.update:
            config.update_package(version=args.update)
            return 0

        self.parser.print_help()
        return 0
