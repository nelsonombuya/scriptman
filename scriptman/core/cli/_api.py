from argparse import ArgumentParser, Namespace, _SubParsersAction

from scriptman.core.cli._parser import BaseParser
from scriptman.core.config import config
from scriptman.powers.api import api


class APISubParser(BaseParser):
    def __init__(self, sub_parser: "_SubParsersAction[ArgumentParser]") -> None:
        """
        🚀 Initialize an APISubParser instance with an ArgumentParser.

        Args:
            sub_parser: ArgumentParser instance to use for parsing CLI arguments.
        """

        self.parser: ArgumentParser = sub_parser.add_parser(
            "api", help="Start the API server with uvicorn"
        )

        # Initialize sub-commands
        self.api()

    @property
    def command(self) -> str:
        """
        ⚙ Get the name of the command being parsed.

        Returns:
            str: The name of the command being parsed.
        """
        return "api"

    def api(self) -> None:
        """
        📝 Add arguments for starting the API server.
        """
        self.parser.add_argument(
            "--init",
            default=False,
            action="store_true",
            help="Initialize the api module needed for the api endpoints.",
        )
        self.parser.add_argument(
            "--host",
            default="0.0.0.0",
            help="Host to bind to. Defaults to 0.0.0.0",
        )
        self.parser.add_argument(
            "--port",
            type=int,
            default=api._find_available_port(),
            help="Port to use. Defaults to an available port",
        )
        self.parser.add_argument(
            "--start",
            default=False,
            action="store_true",
            help="Start the API server",
        )

    def process(self, args: Namespace) -> int:
        """
        ⚙ Process parsed CLI arguments for the 'api' sub-command.

        This function takes the parsed CLI arguments as a Namespace object and
        runs the FastAPI server based on the provided options.

        Args:
            args (Namespace): Parsed CLI arguments containing the following attributes:

                - init (bool): Initialize the api module needed for the api endpoints.
                - host (str): Host to bind to.
                - port (int): Port to use.
                - start (bool): Start the API server.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """
        if args.init:
            api.initialize_api_module()

        if args.start:
            from pathlib import Path
            from runpy import run_path
            from sys import path as sys_path

            api_file_path = Path(config.cwd) / "api.py"

            if str(api_file_path.parent) not in sys_path:
                sys_path.insert(0, str(api_file_path.parent))

            run_path(str(api_file_path), run_name="__main__")

        return 0
