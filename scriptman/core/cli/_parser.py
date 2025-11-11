from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace


class BaseParser(ABC):
    def __init__(self, parser: ArgumentParser) -> None:
        """
        🚀 Initialize a parser with a shared ArgumentParser instance.

        Args:
            parser: ArgumentParser instance used to define CLI arguments.
        """
        self.parser: ArgumentParser = parser
        self.configure()

    @property
    @abstractmethod
    def command(self) -> str:
        """
        ⚙️ Retrieve the command name registered by this parser.

        Returns:
            Command keyword handled by this parser.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def configure(self) -> None:
        """
        ⚙️ Declare CLI arguments for this parser instance.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def process(self, args: Namespace) -> int:
        """
        ⚙️ Process parsed CLI arguments according to the parser's configuration.

        Args:
            args: Parsed CLI arguments as a Namespace object.

        Returns:
            Exit code (0 for success, non-zero for failure).
        """
        raise NotImplementedError("Subclasses must implement this method")
