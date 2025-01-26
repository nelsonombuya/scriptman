from os import getcwd
from pathlib import Path
from re import Pattern, compile
from subprocess import PIPE, run
from typing import Literal

from loguru import logger
from pydantic import BaseModel, Field
from tomlkit import dumps, parse


# NOTE: Do not use emoji in this file
class Version(BaseModel):
    """
    Version Class

    Represents the version of the application as major, minor, and commit.
    """

    major: int = Field(default=2, description="Major version number")
    minor: int = Field(default=0, description="Minor version number")
    commit: int = Field(default=0, description="Commit count")

    def __str__(self) -> str:
        """
        Returns the version string as a dotted format (major.minor.commit).

        Returns:
            str: The version string in dotted format.
        """
        return f"{self.major}.{self.minor}.{self.commit}"

    def pattern(self, field: str) -> Pattern:
        """
        Generates a regex pattern to match a version field declaration in the version
        file.

        Args:
            field (str): The name of the version field to match
                (e.g., "major", "minor", "commit").

        Returns:
            Pattern: A compiled regex pattern to find and capture the specified field's
                declaration.
        """
        return compile(rf"(\s*{field}:\s*int\s*=\s*Field\(default=)(\d+)(.*)")

    def update_version_in_file(
        self, part: Literal["major", "minor", "commit"], value: int
    ) -> None:
        """
        Update the part value of the version in the version file to the given value
        without modifying other parts of the file.

        Args:
            part (Literal["major", "minor", "commit"]): The part of the version to update.
            value (int): The new value to set for the part.
        """
        updated_lines = []
        commit_updated = False
        version_file_path = Path(__file__)

        with version_file_path.open("r") as f:
            for line in f:
                # Search for the commit field and replace its default value
                match = self.pattern(part).search(line)
                if match and not commit_updated:
                    new_line = f"{match.group(1)}{value}{match.group(3)}\n"
                    updated_lines.append(new_line)
                    commit_updated = True
                else:
                    updated_lines.append(line)

        if not commit_updated:
            raise ValueError("Commit line not found in version file.")

        # Write the updated content back to the file
        with version_file_path.open("w") as f:
            f.writelines(updated_lines)

    def get_commit_count(self) -> int:
        """
        Retrieve the commit count from Git.

        Returns:
            int: The number of commits in the repository (default 0 on failure).
        """
        try:
            result = run(
                ["git", "rev-list", "--count", "HEAD"],
                stdout=PIPE,
                stderr=PIPE,
                check=True,
                text=True,
            )
            return int(result.stdout.strip())
        except Exception as e:
            logger.error(f"Failed to get commit count: {e}")
            return 0

    def read_version_from_pyproject(self):
        """
        Read the version from the pyproject.toml file.

        Returns:
            str: The version string of the 'scriptman' project.

        Raises:
            FileNotFoundError: If the pyproject.toml file is not found.
            RuntimeError: If the current project is not 'scriptman'.
        """
        pyproject_file = Path(getcwd()) / "pyproject.toml"

        if not pyproject_file.exists():
            raise FileNotFoundError(f"The pyproject.toml not found at {pyproject_file}")

        with pyproject_file.open("r", encoding="utf-8") as f:
            pyproject_data = parse(f.read())

        if pyproject_data.get("tool", {}).get("poetry", {}).get("name") == "scriptman":
            return pyproject_data["tool"]["poetry"]["version"]  # type: ignore
        else:
            raise RuntimeError("The current project is not scriptman!")

    def update_version_on_pyproject(self):
        """
        Updates the version of the 'scriptman' project in the pyproject.toml file.
        This method checks if the pyproject.toml file exists in the current working
        directory.

        If the file exists, it reads the file and parses its content. If the project name
        in the pyproject.toml file is 'scriptman', it updates the version to the value of
        `self.version`.

        The updated content is then written back to the pyproject.toml file.

        Raises:
            FileNotFoundError: If the pyproject.toml file is not found in the current
                working directory.
            RuntimeError: If the project name in the pyproject.toml file is not scriptman.
        """
        pyproject_file = Path(getcwd()) / "pyproject.toml"

        if not pyproject_file.exists():
            raise FileNotFoundError(f"The pyproject.toml not found at {pyproject_file}")

        with pyproject_file.open("r", encoding="utf-8") as f:
            pyproject_data = parse(f.read())

        if pyproject_data.get("tool", {}).get("poetry", {}).get("name") == "scriptman":
            pyproject_data["tool"]["poetry"]["version"] = str(self)  # type: ignore

            with pyproject_file.open("w", encoding="utf-8") as f:
                f.write(dumps(pyproject_data))
        else:
            raise RuntimeError("The current project is not scriptman!")

    @property
    def scriptman(self) -> str:
        scriptman_file = Path(__file__).parent / "logo.txt"
        if not scriptman_file.exists():
            return f"🦸‍♂️ Scriptman v{self} 🦸‍♀️"
        with scriptman_file.open("r", encoding="utf-8") as f:
            return f.read()
