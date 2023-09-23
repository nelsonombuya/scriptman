import sys
from typing import List

from script_manager.handlers.scripts import ScriptsHandler
from script_manager.handlers.settings import settings


class CLIHandler:
    """
    Command Line Interface Handler for managing and running scripts.

    Args:
        args (List[str]): List of command-line arguments.
    """

    def __init__(self, args: List[str]):
        self.scripts = []
        self.calling_file = args[0]
        self.script_handler = ScriptsHandler()
        self._parse_args(args[1:])

        if settings.debug_mode:
            self.script_handler.test_scripts(self.scripts)
        else:
            self.script_handler.run_scripts(self.scripts)

    def _parse_args(self, args: List[str]) -> None:
        """
        Parse command-line arguments and configure script execution settings.

        Args:
            args (List[str]): List of command-line arguments.
        """
        for arg in args:
            if arg in ("-h", "--help"):
                self._print_help()
                sys.exit(0)  # Exit after printing help

            if arg in ("-ls", "--list_scripts"):
                [print(script) for script in self.script_handler.get_scripts()]
                sys.exit(0)  # Exit after printing the list of scripts

            arg_function = {
                "-dl": settings.disable_logging,
                "--disable_logging": settings.disable_logging,
                "-d": settings.enable_debugging,
                "--debug": settings.enable_debugging,
            }.get(arg)

            if arg_function:
                arg_function()
                continue

            if self._is_valid_script_arg(arg):
                self.scripts.append(arg)
                continue

            print(f"Invalid argument {arg} passed! \n\n")
            self._print_help()
            sys.exit(0)  # Exit after having an error.

    def _is_valid_script_arg(self, arg: str) -> bool:
        """
        Check if the argument corresponds to a valid script filename.

        Args:
            arg (str): Argument to be checked.

        Returns:
            bool: True if the argument is a valid script filename,
            False otherwise.
        """
        return any(arg in name for name in self.script_handler.get_scripts())

    def _print_help(self) -> None:
        """
        Print a manual for the various flags that can be used.

        Returns:
            None
        """
        help_message = f"""
        Usage: python {self.calling_file} [options] [script_names]

        Options:
        -h, --help              Show this help message and exit.
        -dl, --disable_logging  Disable logging.
        -d, --debug             Enable debugging mode.
        -ls, --list_scripts     List scripts contained in the scripts folder.

        script_names            Names of the scripts to execute.
        """
        print(help_message)
