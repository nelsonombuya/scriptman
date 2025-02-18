import sys
from argparse import ArgumentParser
from typing import Optional

from loguru import logger

from scriptman.core.cli._api import APISubParser
from scriptman.core.cli._cleanup import CleanUpSubParser
from scriptman.core.cli._config import ConfigSubParser
from scriptman.core.cli._general import GeneralParser
from scriptman.core.cli._parser import BaseParser
from scriptman.core.cli._run import RunSubParser


class CLI:
    """
    🤖 Comprehensive Command Line Interface Handler for managing and running scripts.

    This class provides a flexible, platform-agnostic approach to script management with
    extensive configuration options.
    """

    @staticmethod
    def start_cli_instance(argv: Optional[list[str]] = None) -> int:
        """
        🌟 Start a new instance of the CLIHandler and execute scripts.

        Args:
            argv (Optional[List[str]]): Command-line arguments to pass to the CLI.
                If not provided, defaults to sys.argv[1:].

        Returns:
            int: Exit code from the CLIHandler execution (0 for success, non-zero for
                failure).
        """
        return CLI().run(argv)

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
        subparsers = parser.add_subparsers(
            dest="action",
            help="Action to perform",
        )

        # Sub-Parsers
        self.commands: dict[str, BaseParser] = {
            "config": ConfigSubParser(subparsers),
            "clean": CleanUpSubParser(subparsers),
            "general": GeneralParser(parser),
            "api": APISubParser(subparsers),
            "run": RunSubParser(subparsers),
        }
        return parser

    def run(self, argv: Optional[list[str]] = None) -> int:
        """
        🏃🏾‍♂️ Parse arguments and execute actions based on CLI configuration.

        Args:
            argv (Optional[List[str]]): Command-line arguments.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """
        args = self._create_parser().parse_args(argv or sys.argv[1:])

        try:
            return self.commands.get(args.action, self.commands["general"]).process(args)
        except Exception as e:
            logger.error(f"❌ CLI execution error: {e}")
            return 1
