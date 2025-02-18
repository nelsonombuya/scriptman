from argparse import ArgumentParser, Namespace, _SubParsersAction

from scriptman.core.cli._parser import BaseParser
from scriptman.core.config import config
from scriptman.core.defaults import ConfigModel


class ConfigSubParser(BaseParser):
    def __init__(self, sub_parser: "_SubParsersAction[ArgumentParser]") -> None:
        """
        🚀 Initialize a ConfigSubParser instance with an ArgumentParser.

        Args:
            sub_parser: ArgumentParser instance to use for parsing CLI arguments.
        """

        self.parser: ArgumentParser = sub_parser.add_parser(
            "config", help="Manage scriptman settings"
        )

        # Initialize sub-commands
        self.config()

    @property
    def command(self) -> str:
        """
        ⚙ Get the name of the command being parsed.

        Returns:
            str: The name of the command being parsed.
        """
        return "config"

    def config(self) -> None:
        """
        📝 Set configuration parameter to a specified value.

        Args:
            config: The configuration parameter to update.
            value: The value to set for the specified configuration parameter.
        """
        self.parser.add_argument(
            "-s",
            "--set",
            nargs=2,
            metavar=("CONFIG", "VALUE"),
            help="Set configuration parameter to a specified value.\n"
            "NOTE: For configuration parameters that take lists, "
            "this will append the value to the list",
        )

        self.parser.add_argument(
            "-r",
            "--reset",
            nargs=1,
            metavar="CONFIG",
            choices=ConfigModel.model_fields,
            help="Reset configuration parameter to default value.\n"
            "NOTE: For configuration parameters that take lists, "
            "this will remove all values from the list",
        )

        self.parser.add_argument(
            "-l",
            "--list",
            action="store_true",
            help="List all current configuration parameters and their values",
        )

    def process(self, args: Namespace) -> int:
        """
        ⚙ Process parsed CLI arguments for the 'config' sub-command.

        This function takes the parsed CLI arguments as a Namespace object and
        updates configuration parameters based on the provided options.

        Args:
            args (Namespace): Parsed CLI arguments containing the following attributes:

                - set (list[str]): A list of two strings: the parameter name and value.
                - reset (str): The parameter name to reset to its default value.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """
        if args.set:
            param, value = args.set
            return int(not config.validate_and_update_configuration(param, value))

        if args.reset:
            param = args.reset[0]
            return int(not config.reset_configuration(param))

        if args.list:
            for param, value in config._configs.model_dump().items():
                print(f"\n\t- {param}: {value}")

        return 1
