from argparse import ArgumentParser, Namespace, _SubParsersAction
from importlib import import_module
from pkgutil import iter_modules

from loguru import logger

from scriptman.core.cli._parser import BaseParser
from scriptman.powers.api import api_manager


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
            default=api_manager._find_available_port(),
            help="Port to use. Defaults to an available port",
        )
        self.parser.add_argument(
            "--modules",
            nargs="+",
            help="List of modules to import. "
            "If not specified, will auto-import all modules in the `api` package",
        )

    def auto_import_modules(self, package_name: str) -> None:
        """➕ Automatically import all modules in the given package."""
        try:
            logger.debug(f"📦 Importing package: {package_name}")
            package = import_module(package_name)
        except ImportError as e:
            logger.warning(f"📪 Failed to import {package_name}: {e}")
            return  # If the package doesn't exist, skip auto-import.

        if hasattr(package, "__path__"):
            for finder, module_name, is_pkg in iter_modules(package.__path__):
                logger.debug(f"📦 Importing module: {package_name},{module_name}")
                import_module(f"{package_name}.{module_name}")

    def process(self, args: Namespace) -> int:
        """
        ⚙ Process parsed CLI arguments for the 'api' sub-command.

        This function takes the parsed CLI arguments as a Namespace object and
        runs the FastAPI server based on the provided options.

        Args:
            args (Namespace): Parsed CLI arguments containing the following attributes:

                - init (bool): Initialize the api module needed for the api endpoints.
                - modules (list[str]): List of modules to import.
                - host (str): Host to bind to.
                - port (int): Port to use.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """
        if args.init:
            api_manager.initialize_api_module()
            return 0

        if args.modules:
            for module_name in args.modules:  # Import user-specified modules
                logger.debug(f"📦 Importing module: {module_name}")
                import_module(module_name)
        else:
            self.auto_import_modules("api")

        api_manager.run(host=args.host, port=args.port)
        return 0
