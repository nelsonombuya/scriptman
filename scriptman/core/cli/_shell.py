import platform
from pathlib import Path
from typing import Optional

from loguru import logger

from scriptman.core.config import config


class ShellScriptGenerator:
    """
    A class that generates platform-specific shell scripts that activate a virtual
    environment and run scriptman with the provided arguments.
    """

    # Templates for different platforms
    BATCH_TEMPLATE = """
@echo off
set "VENV_ACTIVATE_SCRIPT=%~dp0\\{venv_name}\\Scripts\\activate.bat"
set "VENV_DEACTIVATE_SCRIPT=%~dp0\\{venv_name}\\Scripts\\deactivate.bat"

if exist "%VENV_ACTIVATE_SCRIPT%" (
    echo Activating virtual environment...
    call "%VENV_ACTIVATE_SCRIPT%"
) else (
    echo Virtual environment not found at %VENV_ACTIVATE_SCRIPT%
    exit /b 1
)

:: Call the scriptman entry point from the virtual environment
python "%~dp0\\scriptman\\__run__.py" %*

if exist "%VENV_DEACTIVATE_SCRIPT%" (
    echo Deactivating virtual environment...
    call "%VENV_DEACTIVATE_SCRIPT%"
) else (
    echo Virtual environment not found at %VENV_DEACTIVATE_SCRIPT%
    exit /b 1
)
"""

    SHELL_TEMPLATE = """
#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
VENV_ACTIVATE_SCRIPT="$SCRIPT_DIR/{venv_name}/bin/activate"
VENV_DEACTIVATE_SCRIPT="$SCRIPT_DIR/{venv_name}/bin/deactivate"
if [ -f "$VENV_ACTIVATE_SCRIPT" ]; then
    echo "Activating virtual environment..."
    source "$VENV_ACTIVATE_SCRIPT"
else
    echo "Virtual environment not found at $VENV_ACTIVATE_SCRIPT"
    exit 1
fi

scriptman "$@"

if [ -f "$VENV_DEACTIVATE_SCRIPT" ]; then
    echo "Deactivating virtual environment..."
    source "$VENV_DEACTIVATE_SCRIPT"
fi
"""

    POWERSHELL_TEMPLATE = """
$ROOT_DIR = $PSScriptRoot
$VENV_ACTIVATE_SCRIPT = "$ROOT_DIR\\{venv_name}\\Scripts\\Activate.ps1"

if (Test-Path $VENV_ACTIVATE_SCRIPT) {{
    Write-Host "Activating virtual environment..."
    & $VENV_ACTIVATE_SCRIPT
}} else {{
    Write-Error "Virtual environment not found at $VENV_ACTIVATE_SCRIPT"
    exit 1
}}

scriptman $args

Write-Host "Deactivating virtual environment..."
deactivate
"""

    @classmethod
    def get_script_templates(cls) -> dict[str, str]:
        """
        Returns a dictionary of script templates for different platforms.

        Returns:
            dict[str, str]: Dictionary with platform names as keys and script templates as
                values.
        """
        return {
            "windows_powershell": cls.POWERSHELL_TEMPLATE,
            "windows": cls.BATCH_TEMPLATE,
            "darwin": cls.SHELL_TEMPLATE,  # macOS uses the same shell script as Linux
            "linux": cls.SHELL_TEMPLATE,
        }

    @staticmethod
    def get_platform_type() -> str:
        """
        Determines the current platform type.

        Returns:
            str: The platform type (windows, linux, or darwin)
        """
        system = platform.system().lower()
        if system == "windows":
            return system
        elif system == "linux":
            return system
        elif system == "darwin":
            return system
        else:
            return "linux"  # Default to Linux for unknown systems

    @staticmethod
    def get_file_extension(platform_type: str) -> str:
        """
        Returns the file extension for the given platform type.
        """
        return {
            "windows": ".bat",
            "windows_powershell": ".ps1",
            "darwin": ".sh",
            "linux": ".sh",
        }.get(platform_type, ".sh")

    @classmethod
    def generate_runner_script(
        cls,
        relative_venv_path: str = ".venv",
        platform_type: Optional[str] = None,
    ) -> str:
        """
        Generates a runner script for the specified platform that activates a virtual
        environment and runs scriptman with the provided arguments.

        Args:
            relative_venv_path (str): Path to the virtual environment relative to the
                project root.
            platform_type (Optional[str]): The platform to generate the script for. If
                None, uses the current platform.

        Returns:
            str: The generated script content
        """

        if platform_type is None:
            platform_type = cls.get_platform_type()

        templates = cls.get_script_templates()

        if platform_type == "windows" and platform_type in templates:
            return templates[platform_type].format(venv_name=relative_venv_path)
        elif platform_type == "windows_powershell" and platform_type in templates:
            return templates[platform_type].format(venv_name=relative_venv_path)
        elif platform_type in templates:
            return templates[platform_type].format(venv_name=relative_venv_path)
        else:
            # Default to Linux script if platform not recognized
            return templates["linux"].format(venv_name=relative_venv_path)

    @classmethod
    def write_script(
        cls,
        script_content: Optional[str] = None,
        platform_type: Optional[str] = None,
        relative_venv_path: str = ".venv",
        filename: str = "scriptman",
    ) -> None:
        """
        Writes the generated script content to a file.
        """
        if script_content is None:
            script_content = cls.generate_runner_script(
                relative_venv_path=relative_venv_path,
                platform_type=platform_type,
            )

        if platform_type is None:
            platform_type = cls.get_platform_type()

        extension = cls.get_file_extension(platform_type)
        file_path = Path(config.cwd) / f"{filename}{extension}"
        file_path.write_text(script_content)

        if platform_type not in ["windows", "windows_powershell"]:
            # Make the shell script executable on Unix-like systems
            from os import chmod

            chmod(file_path, 0o755)

        logger.success(f"✨ Runner script generated at {file_path}")
