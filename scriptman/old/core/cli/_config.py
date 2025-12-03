from argparse import ArgumentParser, Namespace, _SubParsersAction

from scriptman.core.cli._parser import BaseParser
from scriptman.core.config import config
from scriptman.core.config._defaults import ConfigModel


class ConfigSubParser(BaseParser):
    def __init__(self, sub_parser: "_SubParsersAction[ArgumentParser]") -> None:
        """
        🚀 Initialize the config command parser with shared argument wiring.

        Args:
            sub_parser: Subparser collection to register the command against.
        """

        parser = sub_parser.add_parser("config", help="Manage scriptman settings")
        super().__init__(parser)

    @property
    def command(self) -> str:
        """
        ⚙ Get the name of the command being parsed.

        Returns:
            str: The name of the command being parsed.
        """
        return "config"

    def configure(self) -> None:
        """⚙️ Declare flags for managing Scriptman configuration."""
        self.parser.add_argument(
            "-s",
            "--set",
            nargs=2,
            metavar=("CONFIG", "VALUE"),
            help=(
                "Set configuration parameter to a specified value. "
                "Supports dot notation (e.g. tasks.idle_timeout 5)."
            ),
        )

        self.parser.add_argument(
            "-r",
            "--reset",
            nargs=1,
            metavar="CONFIG",
            help=(
                "Reset configuration parameter to its default value. "
                "Supports dot notation (e.g. tasks.idle_timeout)."
            ),
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
        result: int = 0
        param: str = ""
        value: str = ""

        if hasattr(args, "set") and args.set:
            param, value = args.set
            param = param.lower()
            result = int(not config.validate_and_update_configuration(param, value))

        if hasattr(args, "reset") and args.reset:
            try:
                param = args.reset[0].lower()
                ConfigModel.get_field_info(param)
                config.settings.reset(param, True)
            except KeyError:
                print(f"⚠️ Unknown configuration key: {param}")
                result = 1

        if hasattr(args, "list") and args.list:
            for param, value in sorted(config.settings.items()):
                print(f"🔍 {param} = {value}")

        return result
