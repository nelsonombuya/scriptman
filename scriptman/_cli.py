"""
ScriptMan - Command Line Interface Handler

This module provides the CLIHandler class, responsible for managing and running
scripts via the command line interface (CLI).

Usage:
- Import the CLIHandler class from this module.
- Create an instance of CLIHandler, passing a list of command-line arguments.
- The CLIHandler instance will parse the arguments and execute scripts
accordingly.

Example:
```python
import sys
from scriptman._cli import CLIHandler

cli_handler = CLIHandler(sys.argv)
# Scripts are executed based on the provided command-line arguments.
```

Classes:
- `CLIHandler`: Handles script execution and configuration via command-line
arguments.

For detailed documentation and examples, please refer to the package
documentation.
"""

import re
import sys
from typing import List

from scriptman._scripts import ScriptsHandler
from scriptman._settings import Settings


class CLIHandler:
    """
    Command Line Interface Handler for managing and running scripts.

    Args:
        args (List[str]): List of command-line arguments.
    """

    def __init__(self, args: List[str]):
        """
        Initialize the CLIHandler and execute scripts based on the provided
        command-line arguments.

        Args:
            args (List[str]): List of command-line arguments.
        """
        self.scripts = []
        self._parse_args(args[1:])
        self.calling_file = args[0]
        self.script_handler = ScriptsHandler()
        Settings.print_logs_to_terminal = False  # To Avoid Duplicate Logging
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

            if arg.startswith("-venv"):
                env_match = re.match(r'-venv=["\']?(.*?)(?=["\']|$)', arg)
                env_folder = env_match.group(1) if env_match else ".venv"
                Settings.use_venv(env_folder)  # Enabling the local environment

            arg_function = {
                "-dl": Settings.disable_logging,
                "--disable_logging": Settings.disable_logging,
                "-d": Settings.enable_debugging,
                "--debug": Settings.enable_debugging,
                "-upg": Settings.upgrade_scriptman,
                "--upgrade": Settings.upgrade_scriptman,
                "-upd": Settings.update_scripts,
                "--update": Settings.update_scripts,
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
        -upg, --upgrade         Upgrade ScriptMan.
        -upd, --update          Update Scripts Repository with latest commit.
        -venv[=name]            Enable a local virtual Python environment.
                                By default uses '.venv', but one can specify
                                the environment name using -venv=name or
                                -venv="name".

        script_names            Names of the scripts to execute.
        """
        print(help_message)
