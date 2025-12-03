from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path
from typing import Optional

from scriptman.core.cli._parser import BaseParser
from scriptman.core.config import config
from scriptman.powers.api import api


class APISubParser(BaseParser):
    def __init__(self, sub_parser: "_SubParsersAction[ArgumentParser]") -> None:
        """
        🚀 Initialize the API command parser.

        Args:
            sub_parser: Subparser collection to register the command against.
        """
        parser = sub_parser.add_parser(
            "api",
            help="Start the API server with uvicorn",
        )
        super().__init__(parser)

    @property
    def command(self) -> str:
        """
        ⚙ Get the name of the command being parsed.

        Returns:
            str: The name of the command being parsed.
        """
        return "api"

    def configure(self) -> None:
        """⚙️ Add arguments for starting the API server."""
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
            "--file",
            default=None,
            help="Custom API file path to use. Prompts when not provided.",
        )
        self.parser.add_argument(
            "--show-config",
            default=False,
            action="store_true",
            help="Display the currently configured default API file.",
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
        exit_code = 0
        if args.show_config:
            self._show_configuration()
            return exit_code

        if args.init:
            exit_code = self._initialize_api_defaults(args)

        if args.start:
            self._start_api_server(args)

        if not args.init and not args.start and not args.show_config:
            self.parser.print_help()

        return exit_code

    def _initialize_api_defaults(self, args: Namespace) -> int:
        """
        🔍 Initialize the API defaults.

        Args:
            args: Namespace object containing the parsed CLI arguments.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """
        try:
            module_path = self._resolve_api_path(args.file)
        except FileNotFoundError as exc:
            print(f"⚠️ {exc}")
            return 1

        relative_path = self._to_relative_path(module_path)
        config.secrets.set("api.file", relative_path, write_to_file=True)
        config.secrets.set("api.host", args.host, write_to_file=True)
        config.secrets.set("api.port", args.port, write_to_file=True)
        api.initialize_api_module(str(module_path))
        print(
            "✅ API defaults saved\n"
            "--------------------------------------------------\n"
            f"💡 File: {relative_path}\n"
            f"💡 Host: {args.host}\n"
            f"💡 Port: {args.port}\n"
            "--------------------------------------------------\n"
            "💡 Update later: scriptman api --init --file path/to/api.py\n"
        )
        return 0

    def _start_api_server(self, args: Namespace) -> None:
        """
        🚀 Start the API server.

        Args:
            args: Namespace object containing the parsed CLI arguments.
        """
        host = args.host
        port = args.port
        override_path = args.file

        module_path = (
            self._resolve_api_path(override_path)
            if override_path
            else self._ensure_default_module()
        )

        config.secrets.set("api.host", host, write_to_file=True)
        config.secrets.set("api.port", port, write_to_file=True)
        self._run_module(module_path, host, port)

    def _ensure_default_module(self) -> Path:
        """
        🔍 Ensure the default API module is set.

        Returns:
            Path: The resolved path.
        """
        if stored_path := config.secrets.get("api.file"):
            return Path(config.cwd / stored_path).resolve()

        print("⚠️ No default API file configured.\n" "💡 Let's set one up now.")
        module_path = self._resolve_api_path(None)
        relative_path = self._to_relative_path(module_path)
        config.secrets.set("api.file", relative_path, write_to_file=True)
        print(
            "✅ API file stored for future runs\n"
            "--------------------------------------------------\n"
            f"💡 File: {relative_path}\n"
            "--------------------------------------------------"
        )
        return module_path

    def _resolve_api_path(self, supplied: Optional[str]) -> Path:
        """
        🔍 Resolve the API path.

        Args:
            supplied: The path to the API module.

        Returns:
            Path: The resolved path.
        """
        if supplied:
            candidate = Path(supplied)
        else:
            prompt = input(
                "✍️ Provide the path to your FastAPI module (e.g. app/api.py): "
            ).strip()
            candidate = Path(prompt)

        candidate = candidate.expanduser()
        if not candidate.is_absolute():
            candidate = (config.cwd / candidate).resolve()
        else:
            candidate = candidate.resolve()

        if not candidate.exists():
            raise FileNotFoundError(
                f"API module not found at {candidate}. "
                "Use scriptman api --init --file <path> to configure it."
            )

        return candidate

    @staticmethod
    def _to_relative_path(path: Path) -> str:
        """
        🔍 Convert a path to a relative path.

        Args:
            path: Path to convert to a relative path.

        Returns:
            str: The relative path.
        """
        try:
            return str(path.relative_to(config.cwd))
        except ValueError:
            from os import path as ospath

            return ospath.relpath(path, config.cwd)

    def _run_module(self, module_path: Path, host: str, port: int) -> None:
        """
        🚀 Run the module.

        Args:
            module_path: Path to the module to run.
            host: Host to bind to.
            port: Port to use.
        """
        from runpy import run_path
        from sys import path as sys_path

        try:
            from uvicorn import run as uvicorn_run
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "⚠️ Uvicorn is not installed. Install it with: pip install scriptman[api]"
            ) from exc

        if str(module_path.parent) not in sys_path:
            sys_path.insert(0, str(module_path.parent))

        module_globals = run_path(str(module_path))
        application = module_globals.get("app")
        if application is None:
            raise RuntimeError(
                f"⚠️ FastAPI application named 'app' not found in {module_path}"
            )

        print(
            "🚀 Starting FastAPI server\n"
            "--------------------------------------------------\n"
            f"💡 File: {module_path}\n"
            f"💡 Host: {host}\n"
            f"💡 Port: {port}\n"
            "--------------------------------------------------"
        )
        uvicorn_run(application, host=host, port=port, reload=False)

    def _show_configuration(self) -> None:
        """🔍 Show the current API configuration"""
        stored_path = config.secrets.get("api.file")
        if not stored_path:
            print(
                "🔍 No default API file configured. Run scriptman api --init to set one."
            )
            return
        host = config.secrets.get("api.host", "0.0.0.0")
        port = config.secrets.get("api.port", api._find_available_port())
        print(
            "🔍 Current API configuration\n"
            "--------------------------------------------------\n"
            f"💡 File: {stored_path}\n"
            f"💡 Host: {host}\n"
            f"💡 Port: {port}\n"
            "--------------------------------------------------"
        )
