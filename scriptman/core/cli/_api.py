from argparse import ArgumentParser, Namespace, _SubParsersAction

from scriptman.core.cli._parser import BaseParser
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
        self.parser.add_argument(
            "--reload",
            default=False,
            action="store_true",
            help="Enable auto-reload mode to watch for file changes",
        )
        self.parser.add_argument(
            "--workers",
            type=int,
            default=None,
            help="Number of workers to use. Defaults to None",
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
                - reload (bool): Enable auto-reload mode to watch for file changes.
                - workers (int): Number of workers to use.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """
        if args.init:
            api.initialize_api_module()

        if args.start:
            api.run(
                host=args.host,
                port=args.port,
                reload=args.reload,
                workers=args.workers,
            )

        return 0
