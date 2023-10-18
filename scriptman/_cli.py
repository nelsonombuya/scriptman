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

from typing import Callable, List, Optional

from scriptman._scripts import ScriptsHandler
from scriptman._settings import Settings


class CLIHandler:
    """
    Command Line Interface Handler for managing and running scripts.

    Args:
        args (List[str]): List of command-line arguments.
    """

    def __init__(
        self,
        args: List[str],
        upon_failure: Optional[Callable[[str, str], None]] = None,
    ):
        """
        Initialize the CLIHandler and execute scripts based on the provided
        command-line arguments.

        Args:
            args (List[str]): List of command-line arguments.
            upon_failure (callable([str, str], None), optional): A function to
                call upon script execution failure. It should take 2 string
                arguments, where it will receive the script name and stacktrace
                respectively. It should also return None.
        """
        self.script_handler = ScriptsHandler(upon_failure=upon_failure)
        self.disable_logging = False
        self.custom = False
        self.debug = False
        self.force = False
        self.scripts = []
        self._parse_args(args)

        if self.custom:
            self.script_handler.run_custom_scripts(self.scripts, self.force)
        else:
            self.script_handler.run_scripts(self.scripts, self.force)

    def _parse_args(self, args: List[str]) -> None:
        """
        Parse command-line arguments and configure script execution settings.

        Args:
            args (List[str]): List of command-line arguments.
        """
        self.debug, self.custom, self.disable_logging, self.force = map(
            lambda arg: arg.lower() == "true",
            args[1:5],
        )

        if self.debug:
            Settings.enable_debugging()

        if self.disable_logging:
            Settings.disable_logging()

        if self.custom:
            self.scripts.extend(args[5:])
            return

        for script in args[5:]:
            if self._is_valid_script_arg(script):
                self.scripts.append(script)
            else:
                raise ValueError(f"'{script}' not found in scripts folder")

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
