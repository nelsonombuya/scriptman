from argparse import ArgumentParser, Namespace, _SubParsersAction

from scriptman.core.cli._parser import BaseParser
from scriptman.powers.cleanup import CleanUp


class CleanUpSubParser(BaseParser):

    def __init__(self, sub_parser: "_SubParsersAction[ArgumentParser]") -> None:
        """
        🚀 Initializes a CleanUpSubParser instance with an ArgumentParser.

        Args:
            sub_parser: ArgumentParser instance to use for parsing CLI arguments.
        """

        self.parser: ArgumentParser = sub_parser.add_parser(
            "clean", help="Clean up cache, downloaded files, and logs older than 30 days."
        )

        # Initialize sub-commands
        self.cleaner = CleanUp()
        self.clean()

    @property
    def command(self) -> str:
        """
        ⚙ Get the name of the command being parsed.

        Returns:
            str: The name of the command being parsed.
        """
        return "clean"

    def clean(self) -> None:
        """
        ⚙ Add arguments for running scripts with advanced configuration options.

        This function adds the following arguments to the CLI parser:


        """
        self.parser.add_argument(
            "-c",
            "--cache",
            default=False,
            action="store_true",
            help="Clean up all cache files",
        )
        self.parser.add_argument(
            "-s",
            "--selenium",
            default=False,
            action="store_true",
            help="Clean up all downloaded selenium files",
        )
        self.parser.add_argument(
            "-mp",
            "--mypy",
            default=False,
            action="store_true",
            help="Clean up all mypy cache files",
        )

    def process(self, args: Namespace) -> int:
        """
        ⚙ Process parsed CLI arguments for the 'clean' sub-command.

        This function takes the parsed CLI arguments as a Namespace object and runs the
        specified scripts with the given configuration options.

        Args:
            args (Namespace): Parsed CLI arguments containing the following attributes:
                - c, --cache (bool): Clean up all cache files.
                - s, --selenium (bool): Clean up all downloaded selenium files.
                - mp --mypy (bool): Clean up all mypy cache files.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """

        if args.cache:
            self.cleaner.diskcache_cleanup()

        if args.selenium:
            self.cleaner.selenium_cleanup()

        if args.mypy:
            self.cleaner.mypy_cleanup()

        self.cleaner.cleanup()
        return 0
