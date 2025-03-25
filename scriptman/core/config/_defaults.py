from os import getcwd
from pathlib import Path
from typing import Any, Literal, Optional, cast

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
    downloads_dir: DirectoryPath = Field(
        default=Path(__file__).parent.parent / ".downloads",
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
    relative_venv_path: str = Field(
        default=".venv",
        description="Path to the virtual environment relative to the project root",
    )

    @field_validator("logs_dir", "downloads_dir", mode="before")
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

    @classmethod
    def _traverse_model(cls, key_path: str) -> tuple[Optional[BaseModel], Optional[str]]:
        """
        🔍 Traverse the model structure using dot notation to find a field.

        Args:
            key_path (str): Dot-notation path to the field

        Returns:
            tuple[Optional[BaseModel], Optional[str]]: (parent model, field name) or
                (None, None) if not found
        """
        keys = key_path.split(".")
        current_model: BaseModel = cast(BaseModel, cls)

        if len(keys) == 1:
            return current_model, keys[0]

        *parent_keys, last_key = keys
        for key in parent_keys:
            try:
                field = current_model.model_fields[key]
                if hasattr(field.annotation, "model_fields"):
                    current_model = cast(BaseModel, field.annotation)
                else:
                    return None, None
            except (KeyError, AttributeError):
                return None, None

        return current_model, last_key

    @classmethod
    def get_field_info(cls, key_path: str) -> tuple[Any, str, Any]:
        """
        📄 Get field information using dot notation.

        Args:
            key_path (str): Dot-notation path to the field

        Returns:
            tuple[Any, str, Any]: (field type, description, default value)

        Raises:
            KeyError: If the field is not found
        """
        model, field_name = cls._traverse_model(key_path)
        if not model or not field_name:
            raise KeyError(f"Field not found: {key_path}")

        try:
            field = model.model_fields[field_name]
            return (field.annotation, field.description or "", field.get_default())
        except (KeyError, AttributeError) as e:
            raise KeyError(f"Error accessing field {key_path}: {str(e)}")

    @classmethod
    def get_default(cls, key_path: str) -> Any:
        """
        🔍 Get the default value for a field using dot notation.

        Args:
            key_path (str): Dot-notation path to the field

        Returns:
            Any: The default value

        Raises:
            KeyError: If the field is not found
        """
        _, _, default = cls.get_field_info(key_path)
        return default

    @classmethod
    def get_description(cls, key_path: str) -> str:
        """
        🔍 Get the field description using dot notation.

        Args:
            key_path (str): Dot-notation path to the field

        Returns:
            str: The field description

        Raises:
            KeyError: If the field is not found
        """
        _, description, _ = cls.get_field_info(key_path)
        return description

    @classmethod
    def get_type(cls, key_path: str) -> Any:
        """
        🔍 Get the field type using dot notation.

        Args:
            key_path (str): Dot-notation path to the field

        Returns:
            Any: The field type annotation

        Raises:
            KeyError: If the field is not found
        """
        field_type, _, _ = cls.get_field_info(key_path)
        return field_type
