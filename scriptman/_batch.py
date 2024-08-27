"""
ScriptMan - A Python Package for Script Management

ScriptMan is a comprehensive Python package that offers a wide range of tools
and utilities for efficiently managing Python scripts. Whether you're dealing
with data processing, databases, command-line interfaces, web automation, or
simply need better organization for your scripts, ScriptMan provides the
solutions you need.

This module contains the batch file content for ScriptMan.

"""


def get_batch_file_content() -> str:
    """
    Function to return the content of the batch file as a string.

    Returns:
        str: Content of the batch file.
    """
    import os

    # Get the directory of the current script (_batch.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the path to the _scriptman.bat file
    bat_file_path = os.path.join(current_dir, "_scriptman.bat")

    # Read the content of the batch file
    with open(bat_file_path, "r") as file:
        batch_content = file.read()

    return batch_content


# flake8: noqa: E501 # NOTE: Ignore Line Length for this file
BATCH_FILE: str = r"""@echo off
:: -----------------------------------------------------------------------------
:: SCRIPTMAN [0.0.0.74]
:: -----------------------------------------------------------------------------
:: Companion Batch File for the ScriptMan Package. See scriptman.CLIHandler for
:: more.
:: -----------------------------------------------------------------------------
:: Usage:
::   sm.bat         [-h | --help]
::                  [-q | --quick]
::                  [-d | --debug]
::                  [-f | --force]
::                  [-c | --custom]
::                  [-cl | --clearlock]
::                  [-dl | --disable_logging]
::                  [-i=script1 | --ignore=script1] [-i=script2 | --ignore=script2] ...
::
::                  [script_file [script_file ...]]
::
:: Options:
::   -h, --help             Show this help message and exit (skips all other flags).
::   -q, --quick            Enable quick mode (skips updates and installation).
::   -d, --debug            Enable debugging mode.
::   -f, --force            Force the scripts to run even if there's another running instance.
::   -c, --custom           Enable custom mode (specify custom script files).
::   -cl, --clearlock       Clear a specific, or all the lock files.
::   -dl, --disable_logging Disable logging.
::   -i, --ignore           Specify a script to ignore. Can be used multiple times.
::
::   script_file            One or more script files to run.
::
:: Example:
::   .\sm.bat -q -d -dl -i=script1 -i="script2.py" script3 "script4.py"
::
:: Note:
:: Ensure to define the following in the scriptman batch file:
::      1. MAIN_SCRIPT: The name of your project's entry point file (e.g. __main__.py)
::      2. VENV: The name venv folder in your project.
::      3. ROOT_DIR [Optional]: The root dir of your project. If left blank, will default to the current directory for the scriptman.bat file.
:: -----------------------------------------------------------------------------
setlocal enabledelayedexpansion

set "ROOT_DIR={ROOT_DIR}"
set "VENV_NAME={VENV_NAME}"
set "MAIN_SCRIPT={MAIN_SCRIPT}"

set "SCRIPTS="
set "QUICK=False"
set "DEBUG=False"
set "FORCE=False"
set "CUSTOM=False"
set "CLEARLOCK=False"
set "DISABLE_LOGGING=False"
set "IGNORE_SCRIPTS="
if "!ROOT_DIR!" == "" set "ROOT_DIR=%~dp0"

:process_args
for %%A in (%*) do (
    if /I "%%~A" == "-h" (
        call :show_help
        exit /b
    )

    if /I "%%~A" == "--help" (
        call :show_help
        exit /b
    )
)

if "%~1" == "" goto run_script

set "arg=%~1"
if "%arg:~0,1%" == "-" (
    if "%arg%" == "-i" (
        if defined IGNORE_SCRIPTS (
            set "IGNORE_SCRIPTS=!IGNORE_SCRIPTS!,"%~2""
        ) else (
            set "IGNORE_SCRIPTS="%~2""
        )
        shift
    ) else if "%arg%" == "--ignore" (
        if defined IGNORE_SCRIPTS (
            set "IGNORE_SCRIPTS=!IGNORE_SCRIPTS!,"%~2""
        ) else (
            set "IGNORE_SCRIPTS="%~2""
        )
    ) else (
        call :handle_flag "%arg%"
    )
) else (
    call :handle_file "%arg%"
)
shift
goto process_args

:handle_flag
set "flag=%~1"
shift

if /I "!flag!" == "-q" (
    set "QUICK=True"
    exit /b
)

if /I "!flag!" == "--quick" (
    set "QUICK=True"
    exit /b
)

if /I "!flag!" == "-c" (
    set "CUSTOM=True"
    exit /b
)

if /I "!flag!" == "--custom" (
    set "CUSTOM=True"
    exit /b
)

if /I "!flag!" == "-d" (
    set "DEBUG=True"
    exit /b
)

if /I "!flag!" == "--debug" (
    set "DEBUG=True"
    exit /b
)

if /I "!flag!" == "-cl" (
    set "CLEARLOCK=True"
    exit /b
)

if /I "!flag!" == "--clearlock" (
    set "CLEARLOCK=True"
    exit /b
)

if /I "!flag!" == "-dl" (
    set "DISABLE_LOGGING=True"
    exit /b
)

if /I "!flag!" == "--disable_logging" (
    set "DISABLE_LOGGING=True"
    exit /b
)

if /I "!flag!" == "-f" (
    set "FORCE=True"
    exit /b
)

if /I "!flag!" == "--force" (
    set "FORCE=True"
    exit /b
)

echo Unrecognized flag: !flag!
exit /b 1

:handle_file
if defined SCRIPTS (
    set "SCRIPTS=!SCRIPTS! "%~1""
) else (
    set "SCRIPTS="%~1""
)
exit /b

:run_script
echo Setting root directory
cd /d "!ROOT_DIR!"
echo.

if not "!VENV_NAME!" == "" (
    echo Activating virtual environment...
    call "!ROOT_DIR!\!VENV_NAME!\Scripts\activate.bat"
    echo.
)

if "!QUICK!" == "False" (
    echo Updating Files...
    git pull
    echo.

    echo Upgrading PIP...
    python.exe -m pip install --upgrade pip
    echo.

    echo Installing Dependencies...
    pip install -r requirements.txt
    echo.
)

echo Running script...
python "!ROOT_DIR!\!MAIN_SCRIPT!" !DEBUG! !CUSTOM! !DISABLE_LOGGING! !FORCE! !CLEARLOCK! "--ignore=!IGNORE_SCRIPTS!" !SCRIPTS!
echo.

if not "!VENV_NAME!" == "" (
    echo Deactivating virtual environment...
    call "!ROOT_DIR!\!VENV_NAME!\Scripts\deactivate.bat"
    echo.
)

echo ScriptMan Process Complete.
exit /b

:show_help
echo Usage:
echo    sm.bat      [-h   ^| --help]
echo                [-q   ^| --quick]
echo                [-d   ^| --debug]
echo                [-f   ^| --force]
echo                [-c   ^| --custom]
echo                [-cl  ^| --clearlock]
echo                [-dl  ^| --disable_logging]
echo                [-i=script1 ^| --ignore=script1] [-i=script2 ^| --ignore=script2] ...
echo.
echo                [script_file [script_file ...]]
echo.
echo Options:
echo    -h, --help              Show this help message and exit (skips all other flags).
echo    -q, --quick             Enable quick mode (skips updates and installation).
echo    -d, --debug             Enable debugging mode.
echo    -f, --force             Force the scripts to run even if there's another running instance.
echo    -c, --custom            Enable custom mode (specify custom script files).
echo    -cl, --clearlock        Clear a specific, or all the lock files.
echo    -dl, --disable_logging  Disable logging.
echo    -i, --ignore            Specify a script to ignore. Can be used multiple times.
echo.
echo    script_file             One or more script files to run.
echo.
echo Example:
echo    .\sm.bat -q -d -dl -i=script1 -i="script2.py" script3 "script4.py"
echo.
echo Note:
echo Ensure to define the following in the scriptman batch file:
echo      1. MAIN_SCRIPT: The name of your project's entry point file (e.g. __main__.py)
echo      2. VENV: The name venv folder in your project.
echo      3. ROOT_DIR [Optional]: The root dir of your project. If left blank, will default to the current directory for the scriptman.bat file.
echo.
exit /b
"""
