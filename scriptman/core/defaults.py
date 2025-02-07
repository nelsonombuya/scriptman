from os import getcwd
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, DirectoryPath, Field


class ConfigModel(BaseModel):
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
    ignore_dirs: list[str] = Field(
        default=[],
        description="List of folders to ignore for script execution.",
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
    log_for_each_script: bool = Field(
        default=False,
        description="Enable logging for each script instance",
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
