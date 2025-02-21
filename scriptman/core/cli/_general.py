from argparse import ArgumentParser, Namespace

from scriptman.core.cli._parser import BaseParser
from scriptman.core.config import config
from scriptman.powers.cleanup import CleanUp


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

    def general_arguments(self) -> None:
        """
        ⚙ Adds general arguments to the CLI parser.

        This includes the following arguments:

        - `-r` or `--reset`: Reset configuration to default values, and delete
            `scriptman.toml` file.
        - `-c` or `--cleanup`: Clean up cache, downloaded files, and logs older than
            30 days.
        - `-u` or `--update`: Update Scriptman Package to the given version.
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
            "-l",
            "--lint",
            action="store_true",
            help="Perform code linting and typechecking on the project files.",
        )

        self.parser.add_argument(
            "-u",
            "--update",
            nargs="?",
            const="latest",
            metavar="VERSION",
            help="Update Scriptman Package to the given version (or 'latest' if not "
            "provided). If you use 'next' as a version, it will bump the commit version "
            "up to the latest version + 1 (e.g., 1.0.0 -> 1.0.1). "
            "NOTE: Kindly use semantic versioning for the version.",
        )

        self.parser.add_argument(
            "-p",
            "--publish",
            action="store_true",
            help="Update Scriptman Package to the next version (bump the commit version "
            "up to the latest version + 1 (e.g., 1.0.0 -> 1.0.1)) and publish it to pypi "
            "using poetry.",
        )

        self.parser.add_argument(
            "-v",
            "--version",
            action="version",
            help="Display version",
            version=f"Scriptman {config.version}",
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
            config.settings.reset_all(True)
            return 0

        if args.cleanup:
            CleanUp().cleanup()
            return 0

        if args.update:
            config.update_package(version=args.update)
            return 0

        if args.publish:
            config.lint()
            config.update_package(version="next")
            config.publish_package()
            return 0

        if args.lint:
            config.lint()
            return 0

        # Display help message
        self.parser.print_help()
        return 0
