from argparse import ArgumentParser, Namespace, _SubParsersAction

from loguru import logger

from scriptman.core.cli._parser import BaseParser
from scriptman.core.cli._shell import ShellScriptGenerator
from scriptman.core.config import config


class InitSubParser(BaseParser):
    """
    Handles project initialization with customization options.
    """

    def __init__(self, sub_parser: "_SubParsersAction[ArgumentParser]") -> None:
        """
        🚀 Initialize the init command parser.

        Args:
            sub_parser: Subparser collection to register the command against.
        """

        parser = sub_parser.add_parser(
            "init",
            help="Initialize a new scriptman project with customization options.",
        )

        super().__init__(parser)

    @property
    def command(self) -> str:
        """
        ⚙ Get the name of the command being parsed.

        Returns:
            str: The name of the command being parsed.
        """
        return "init"

    def configure(self) -> None:
        """
        ⚙️ Add arguments for initializing a new project with customization options.

        This function adds the following arguments to the CLI parser:

        - `--venv`: Set the relative path to the virtual environment.
        - `--powershell`: For Windows users, use PowerShell script instead of batch.
        - `--scripts-dir`: Set the directory where scripts will be stored.
        - `--log-level`: Set the default logging level.
        - `--no-concurrent`: Disable concurrent script execution.
        """
        self.parser.add_argument(
            "--venv",
            metavar="PATH",
            default=".venv",
            help="Set the relative path to the virtual environment (default: .venv).",
        )

        self.parser.add_argument(
            "--powershell",
            action="store_true",
            default=False,
            help="For Windows users, use PowerShell script instead of batch script.",
        )

        self.parser.add_argument(
            "--log-level",
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            default="INFO",
            help="Set the default logging level (default: INFO).",
        )

        self.parser.add_argument(
            "--no-concurrent",
            action="store_true",
            default=False,
            help="Disable concurrent script execution.",
        )
        self.parser.add_argument(
            "--shell",
            action="store_true",
            default=False,
            help=(
                "Generate runner scripts that call the virtualenv python directly "
                "without activating the environment."
            ),
        )

    def process(self, args: Namespace) -> int:
        """
        ⚙ Process parsed CLI arguments for the 'init' command.

        This function takes the parsed CLI arguments as a Namespace object and initializes
        a new project with the specified configuration options.

        Args:
            args (Namespace): Parsed CLI arguments containing initialization options.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """
        # Update configuration with user-specified values
        config.settings.set("log_level", args.log_level)
        config.settings.set("relative_venv_path", args.venv)
        config.settings.set("concurrent", not args.no_concurrent)

        # Determine platform type for script generation
        platform_type: str = ShellScriptGenerator.get_platform_type()
        if platform_type == "windows" and args.powershell:
            platform_type = "windows_powershell"

        # Generate the appropriate shell script
        ShellScriptGenerator.write_script(
            relative_venv_path=args.venv,
            platform_type=platform_type,
        )

        if args.shell:
            ShellScriptGenerator.write_script(
                relative_venv_path=args.venv,
                platform_type=platform_type,
                filename="scriptman-run",
                mode="direct",
            )

        # Create supporting files
        config.add_secrets_to_gitignore()
        config.create_secrets_file()

        # Display success message
        ext = ShellScriptGenerator.get_file_extension(platform_type)
        ext = ".ps1" if args.powershell else ext
        logger.success(
            "✨ Project initialized successfully\n"
            "--------------------------------------------------------------------\n"
            f"💡 Virtual environment path set to: {args.venv}\n"
            f"💡 Logging level set to: {args.log_level}\n"
            f"💡 Concurrent execution: {'Disabled' if args.no_concurrent else 'Enabled'}\n"
            "--------------------------------------------------------------------\n"
            "💡 You can add your secrets to the .secrets.toml file.\n"
            "💡 You can configure settings in pyproject.toml under the "
            "[tool.scriptman] section.\n"
            "💡 Or create a scriptman.toml file in the current working directory.\n"
            "--------------------------------------------------------------------\n\n"
            "💡 You can run the project using the following command (while in venv):\n"
            "--------------------------------------------------------------------\n"
            "scriptman run <script_name> [script_args]\n"
            "sm sh <script_name> [script_args]\n"
            "--------------------------------------------------------------------\n\n"
            "💡 You can run the project using the shell script (while not in venv):\n"
            "---------------------------------------------------------------------\n"
            f"Linux/MacOS: source scriptman.sh run <script_name> [script_args]\n"
            f"Windows: source scriptman.{ext} run <script_name> [script_args]\n"
        )

        if args.shell:
            logger.success(
                "--------------------------------------------------------------------\n"
                "💡 Direct runner (no activation):\n"
                f"Linux/MacOS: ./scriptman-run.sh run <script_name> [script_args]\n"
                f"Windows: scriptman-run{ext} run <script_name> [script_args]\n"
                "--------------------------------------------------------------------\n\n"
            )

        return 0
