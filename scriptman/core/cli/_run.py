from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path

from loguru import logger

from scriptman.core.cli._parser import BaseParser
from scriptman.core.config import config
from scriptman.core.scripts import Scripts


class RunSubParser(BaseParser):

    def __init__(self, sub_parser: "_SubParsersAction[ArgumentParser]") -> None:
        """
        🚀 Initializes a RunSubParser instance with an ArgumentParser.

        Args:
            sub_parser: ArgumentParser instance to use for parsing CLI arguments.
        """

        self.parser: ArgumentParser = sub_parser.add_parser(
            "run", help="Run scripts with advanced configuration options."
        )

        # Initialize sub-commands
        self.run()

    @property
    def command(self) -> str:
        """
        ⚙ Get the name of the command being parsed.

        Returns:
            str: The name of the command being parsed.
        """
        return "run"

    def run(self) -> None:
        """
        ⚙ Add arguments for running scripts with advanced configuration options.

        This function adds the following arguments to the CLI parser:

        - `-s` or `--scripts`: Specify scripts to run. If not defined, will run all python
            scripts in the working directory.
        - `-r` or `--retries`: Specify the number of times to retry running the scripts in
            case of failure. Default is 0 (no retries).
        - `-f` or `--force`: Force execution of scripts even if they are already running.
        """
        self.parser.add_argument(
            "-s",
            "--scripts",
            nargs="*",
            metavar="SCRIPT",
            help="Specify scripts to run. "
            "If not defined, will run all python scripts in the working directory. "
            'Example: -s script1.py script2.py "another script.py" "path/to/script.py"',
        )
        self.parser.add_argument(
            "-r",
            "--retries",
            type=int,
            default=0,
            help="Specify the number of times to retry running the scripts in case of "
            "failure. Default is 0 (no retries).",
        )
        self.parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            default=False,
            help="Force execution of scripts even if they are already running.",
        )

    def process(self, args: Namespace) -> int:
        """
        ⚙ Process parsed CLI arguments for the 'run' sub-command.

        This function takes the parsed CLI arguments as a Namespace object and runs the
        specified scripts with the given configuration options.

        Args:
            args (Namespace): Parsed CLI arguments containing the following attributes:

                - scripts (list[Path]): List of script paths to run.
                - retries (int): Number of times to retry running the scripts in case of
                    failure.
                - force (bool): Force execution of scripts even if they are already
                    running.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """
        print(config.scriptman)  # Just looks cool
        scripts = (
            [Path(script) for script in args.scripts]
            if args.scripts
            else list(Path(config.settings.get("scripts_dir", ".")).glob("*.py"))
        )

        if not scripts:
            logger.error("❓ No scripts found in the current directory.")
            return 1

        config.settings.retries = args.retries if args.retries >= 0 else 0
        config.settings.force = args.force

        try:
            Scripts().run_scripts(scripts)
            return 0
        except Exception as e:
            logger.error(f"❌ An error occurred while running the scripts: {e}")
            return 1
