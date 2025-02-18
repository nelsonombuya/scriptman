from os import getcwd
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, DirectoryPath, Field, FilePath, field_validator


class ConfigModel(BaseModel):
    use_pyproject: bool = Field(
        default=not Path(getcwd()).joinpath("scriptman.toml").exists(),
        description="Use pyproject.toml for setting and loading scriptman configurations",
    )
    settings_file: FilePath = Field(
        default=(
            Path(getcwd()).joinpath("scriptman.toml")
            if Path(getcwd()).joinpath("scriptman.toml").exists()
            else Path(getcwd()).joinpath("pyproject.toml")
        ),
        description="Path to the settings file",
    )
    cwd: DirectoryPath = Field(
        default=Path(getcwd()),
        description="Current working directory",
    )
    logs_dir: DirectoryPath = Field(
        default=Path(__file__).parent.parent / "logs",
        description="Path to the logs directory",
    )
    scripts_dir: str = Field(
        default=getcwd(),
        description="Path to the scripts directory",
    )
    downloads_dir: DirectoryPath = Field(
        default=Path(__file__).parent.parent / "downloads",
        description="Path to the downloads directory",
    )
    concurrent: bool = Field(
        default=True,
        description="Enable concurrent script execution",
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "OFF"] = Field(
        default="INFO",
        description="Set the logging level",
    )
    retries: int = Field(
        default=0,
        description="Number of retries for failed scripts",
    )
    force: bool = Field(
        default=False,
        description="Force execution of scripts even if they are already running",
    )
    selenium_optimizations: bool = Field(
        default=True,
        description="Enable selenium optimizations",
    )
    selenium_local_mode: bool = Field(
        default=True,
        description="Enable selenium local mode (Download and run a local copy of the "
        "browser)",
    )
    chrome_download_url: str = Field(
        default=(
            "https://googlechromelabs.github.io/chrome-for-testing/"
            "known-good-versions-with-downloads.json"
        ),
        description="URL to fetch Chrome download URLs",
    )

    @field_validator("logs_dir", "scripts_dir", "downloads_dir", mode="before")
    @classmethod
    def initialize_directories(cls, value: str) -> str:
        """
        📁 Initialize directories for the scriptman package.
        """
        try:
            path = Path(value)
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
            return value
        except Exception as e:
            raise ValueError(f"Failed to create directory {value}: {e}")
