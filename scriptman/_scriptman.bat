@echo off
:: -----------------------------------------------------------------------------
:: SCRIPTMAN [0.0.0.41]
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
::                  [-dl | --disable_logging]
::
::                  [script_file [script_file ...]]
::
:: Options:
::   -h, --help             Show this help message and exit (skips all other flags).
::   -q, --quick            Enable quick mode (skips updates and installation).
::   -d, --debug            Enable debugging mode.
::   -f, --force            Force the scripts to run even if there's another running instance.
::   -c, --custom           Enable custom mode (specify custom script files).
::   -dl, --disable_logging Disable logging.
::
::   script_file            One or more script files to run.
::
:: Note:
:: Ensure to define the following in the scriptman batch file:
::      1. MAIN_SCRIPT: The name of your project's entry point file (e.g. __main__.py)
::      2. VENV: The name venv folder in your project.
::      3. ROOT_DIR [Optional]: The root dir of your project. If left blank, will default to the current directory for the scriptman.bat file.
:: -----------------------------------------------------------------------------
setlocal enabledelayedexpansion

set "ROOT_DIR="
set "VENV_NAME="
set "MAIN_SCRIPT="

set "SCRIPTS="
set "QUICK=False"
set "DEBUG=False"
set "FORCE=False"
set "CUSTOM=False"
set "DISABLE_LOGGING=False"
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
    call :handle_flag "%arg%"
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
echo Setting current directory
cd /d "%~dp0"
echo.

if not "!VENV_NAME!" == "" (
    echo Activating virtual environment...
    call "!ROOT_DIR!\!VENV_NAME!\Scripts\activate.bat"
    echo.
)

if "!QUICK!" == "" (
    echo Updating Files...
    git pull
    echo.
)

echo Running script...
python "!ROOT_DIR!\!MAIN_SCRIPT!" !DEBUG! !CUSTOM! !DISABLE_LOGGING! !FORCE! !SCRIPTS!
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
echo                [-dl  ^| --disable_logging]
echo.
echo                [script_file [script_file ...]]
echo.
echo Options:
echo    -h, --help              Show this help message and exit (skips all other flags).
echo    -q, --quick             Enable quick mode (skips updates and installation).
echo    -d, --debug             Enable debugging mode.
echo    -f, --force             Force the scripts to run even if there's another running instance.
echo    -c, --custom            Enable custom mode (specify custom script files).
echo    -dl, --disable_logging  Disable logging.
echo.
echo    script_file              One or more script files to run.
echo.
echo Note:
echo Ensure to define the following in the scriptman batch file:
echo      1. MAIN_SCRIPT: The name of your project's entry point file (e.g. __main__.py)
echo      2. VENV: The name venv folder in your project.
echo      3. ROOT_DIR [Optional]: The root dir of your project. If left blank, will default to the current directory for the scriptman.bat file.
echo.
exit /b
