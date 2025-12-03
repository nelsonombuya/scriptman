from argparse import ArgumentParser, Namespace, _SubParsersAction

from scriptman.core.cli._parser import BaseParser
from scriptman.powers.cleanup import CleanUp


class CleanUpSubParser(BaseParser):
    def __init__(self, sub_parser: "_SubParsersAction[ArgumentParser]") -> None:
        """
        🚀 Initialize the cleanup command parser.

        Args:
            sub_parser: Subparser collection to register the command against.
        """

        parser = sub_parser.add_parser(
            "clean",
            help="Clean up cache, downloaded files, and logs older than 30 days.",
        )

        super().__init__(parser)
        self.cleaner = CleanUp()

    @property
    def command(self) -> str:
        """
        ⚙ Get the name of the command being parsed.

        Returns:
            str: The name of the command being parsed.
        """
        return "clean"

    def configure(self) -> None:
        """⚙️ Declare cleanup command options."""
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
        targeted = False

        if args.cache:
            targeted = True
            self.cleaner.diskcache_cleanup()

        if args.selenium:
            targeted = True
            self.cleaner.selenium_cleanup()

        if args.mypy:
            targeted = True
            self.cleaner.mypy_cleanup()

        if targeted:
            self.cleaner.run_registered_hooks()
            return 0

        self.cleaner.cleanup()
        return 0
