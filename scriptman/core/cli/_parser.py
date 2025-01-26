from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace


class BaseParser(ABC):
    def __init__(self, parser: ArgumentParser) -> None:
        """
        🚀 Initializes a Parser instance with an ArgumentParser.

        Args:
            parser: ArgumentParser instance to use for parsing CLI arguments.
        """
        self.parser: ArgumentParser = parser

    @property
    @abstractmethod
    def command(self) -> str:
        """
        ⚙ Get the name of the command being parsed.

        Returns:
            str: The name of the command being parsed.
        """
        pass

    @abstractmethod
    def process(self, args: Namespace) -> int:
        """
        ⚙ Process parsed CLI arguments according to the parser's configuration.

        Args:
            args: Parsed CLI arguments as a Namespace object.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """
        pass
