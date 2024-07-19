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

import sys
from typing import Callable, List, Optional

from scriptman._scripts import ScriptsHandler
from scriptman._settings import Settings


class CLIHandler:
    """
    Command Line Interface Handler for managing and running scripts.

    This class parses command-line arguments and executes scripts accordingly.

    Args:
        args (List[str]): List of command-line arguments.
        upon_failure (Optional[Callable[[str, str], None]]): A function to
            call upon script execution failure. It should take 2 string
            arguments, where it will receive the script name and stacktrace
            respectively. It should also return None.
    """

    def __init__(
        self,
        args: List[str] = sys.argv,
        upon_failure: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        """
        Initialize the CLIHandler.

        Args:
            args (List[str]): List of command-line arguments.
            upon_failure (Optional[Callable[[str, str], None]]): A function to
                call upon script execution failure. It should take 2 string
                arguments, where it will receive the script name and stacktrace
                respectively. It should also return None.
        """
        self.script_handler = ScriptsHandler(upon_failure=upon_failure)
        self._parse_args(args)

    def _get_last_arg(self) -> int:
        """
        Get the index of the last argument in the command-line arguments.

        Returns:
            int: Index of the last argument.
        """
        for i, arg in enumerate(sys.argv[1:]):  # sys.argv[0] is the script.
            if arg.lower() not in ("true", "false"):
                return i + 1  # Accounting for how Python indexes lists.
        return len(sys.argv)

    def _parse_args(self, args: List[str]) -> None:
        """
        Parse command-line arguments and execute scripts accordingly.

        Args:
            args (List[str]): List of command-line arguments.
        """
        last_arg = self._get_last_arg()

        (
            debug,
            custom,
            disable_logging,
            force,
            clear_lock_files,
        ) = map(lambda arg: arg.lower() == "true", args[1:last_arg])

        if debug:
            Settings.enable_debugging()

        if disable_logging:
            Settings.disable_logging()

        if clear_lock_files:
            if args[last_arg + 1 :]:
                for script in args[last_arg + 1 :]:
                    if script in self.script_handler.get_scripts():
                        Settings.clear_lock_files(script)
                    else:
                        raise ValueError(f"Script '{script}' not found.")
            else:
                Settings.clear_lock_files()

        if custom:
            self.script_handler.run_custom_scripts(args[last_arg + 1 :], force)
        else:
            scripts = [
                script.replace(".py", "")
                for script in (args[last_arg + 1 :] or self.script_handler.get_scripts())
            ]
            ignore = [
                script.replace(".py", "")
                for script in args[last_arg].replace("--ignore=", "").split(",")
            ]
            scripts = [script for script in scripts if script not in ignore]
            self.script_handler.run_scripts(scripts, force)
