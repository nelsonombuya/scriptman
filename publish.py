import getpass
import os
import re
import shutil
import subprocess
import sys

from dotenv import load_dotenv


class PackagePublishHelper:
    """
    A utility class for automating the package publishing process.

    Args:
        package_name (str): The name of the Python package.
        version (str): The new version to set for the package.

    Attributes:
        dir (str): The root directory of the Script Manager application.
        package_name (str): The name of the Python package.
        version (str): The new version to set for the package.
        use_dotenv (str): Whether to use the credentials from the dotenv file.
        del_app_folder (str): Whether to delete the local app folder used
            for testing.
    Usage:
        Create an instance of PackagePublishHelper with the package name and
        new version, then call the `run` method to execute the package
        publishing process.

    Example:
        >>> helper = PackagePublishHelper("my_package", "1.0.2")
        >>> helper.run()
    """

    def __init__(
        self,
        package_name: str,
        version: str,
        use_dotenv: str,
        del_app_folder: str,
    ):
        """
        A utility class for automating the package publishing process.

        Args:
            package_name (str): The name of the Python package.
            version (str): The new version to set for the package.
            use_dotenv (str): Whether to use the credentials from the dotenv
                file.
            del_app_folder (str): Whether to delete the local app folder used
                for testing.
        """
        self.del_app_folder = del_app_folder.lower() == "true"
        self.dir = os.path.dirname(__file__)
        self.package_name = package_name
        self.version = version

        if use_dotenv.lower() == "true":
            load_dotenv()
            self.username = os.environ["PYPI_USERNAME"]
            self.password = os.environ["PYPI_PASSWORD"]
        else:
            self.username = input("Enter your PyPI username: ")
            self.password = getpass.getpass("Enter your PyPI password: ")

    @staticmethod
    def get_setup_version():
        """
        Gets the version in 'setup.py'.
        """
        setup_py = "setup.py"
        with open(setup_py, "r") as f:
            lines = f.readlines()

        for line in lines:
            if line.strip().startswith("VERSION ="):
                return line

    def _delete_folder(self, folder_path: str):
        """
        Utility method for deleting folders.

        Args:
            folder_path (str): Path of the folder to delete.
        """
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            print(f"Deleted '{folder_path}' folder.")

    def delete_dist_folder(self):
        """
        Delete the 'dist' folder in the Script Manager directory if it exists.
        """
        dist_folder = os.path.join(self.dir, "dist")
        self._delete_folder(dist_folder)

    def delete_egg_info_folder(self):
        """
        Delete the '{package_name}.egg-info' folder in the Script Manager
        directory if it exists.
        """
        egg_info_dir = os.path.join(self.dir, f"{self.package_name}.egg-info")
        self._delete_folder(egg_info_dir)

    def delete_local_app_folder(self):
        """
        Delete the app folder in the Script Manager that's used for testing if
        it exists.
        """
        app_dir = os.path.join(self.dir, "app")
        if os.path.exists(app_dir) and self.del_app_folder:
            self._delete_folder(app_dir)

    def update_setup_version(self):
        """
        Update the version in 'setup.py' to the specified version.
        """
        setup_py = "setup.py"
        with open(setup_py, "r") as f:
            lines = f.readlines()

        new_lines = []
        for line in lines:
            if line.strip().startswith("VERSION ="):
                new_lines.append(f'VERSION = "{self.version}"\n')
            else:
                new_lines.append(line)

        with open(setup_py, "w") as f:
            f.writelines(new_lines)

        print(f"Updated version in '{setup_py}' to '{self.version}'.")

    def update_app_version(self):
        """
        Update the version in '_settings.py' to the specified version.
        """
        settings_py = r"scriptman\_settings.py"

        with open(settings_py, "r") as file:
            lines = file.readlines()

        # Find the line containing 'self.app_version'
        for i, line in enumerate(lines):
            if "self.app_version" in line:
                lines[i] = "        "  # Adding spaces for tabs
                lines[i] += f'self.app_version: str = "{self.version}"\n'
                break  # Stop searching once we've found and updated the line

        with open(settings_py, "w") as file:
            file.writelines(lines)

        print(f"Updated version in '{settings_py}' to '{self.version}'.")

    def run_build(self):
        """
        Build distribution packages using 'python -m build'.
        """
        subprocess.run(["python", "-m", "pip", "install", "build"])
        subprocess.run(["python", "-m", "build"])
        print("Built distribution packages.")

    def upload_to_twine(self):
        """
        Upload distribution packages to Twine using 'twine upload'.
        """
        subprocess.run(["python", "-m", "pip", "install", "twine"])
        cmd = ["twine", "upload", "dist/*"]
        cmd.extend(["-u", self.username, "-p", self.password])

        subprocess.run(cmd)
        print("Uploaded distribution packages to Twine.")

    def update_batch_file(self):
        """
        Add the sm.bat file contents to the _batch.py file to use during setup.
        """
        pattern = r"::\s+(.*?)\s*\[([\d.]+)\]"
        python_file_path = r"scriptman\_batch.py"
        bat_file_path = r"scriptman\_scriptman.bat"

        # Get Batch File Content
        with open(bat_file_path, "r") as bat_file:
            content = bat_file.readlines()

        # Update version number in batch file
        for i, line in enumerate(content):
            match = re.match(pattern, line)
            if match:
                script_name, old_version = match.groups()

                # Replace the version number with the custom version
                new_line = re.sub(
                    pattern,
                    f":: {script_name} [{self.version}]",
                    line,
                )

                content[i] = new_line
                break

        # Writing the batch file content to the _batch.py file
        python_variable = 'BATCH_FILE: str = r"""' + "".join(content) + '"""\n'
        with open(python_file_path, "w") as python_file:
            python_file.write(python_variable)

        print("Updated Batch File.")

    def run(self):
        """
        Run the package publishing process, including deleting folders,
        updating version, building distribution packages, and uploading to
        Twine.
        """
        self.delete_local_app_folder()
        self.delete_egg_info_folder()
        self.update_setup_version()
        self.update_app_version()
        self.delete_dist_folder()
        self.update_batch_file()
        self.run_build()
        self.upload_to_twine()
        print("Package publishing completed.")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(
            f"""
            Usage:
            python publish.py
                <package_name> <version> <use_dotenv> <del_app_folder>

            {PackagePublishHelper.get_setup_version()}
            """
        )
        sys.exit(1)

    package_name, version, use_dotenv, del_app_folder = sys.argv[1:]
    PackagePublishHelper(
        package_name,
        version,
        use_dotenv,
        del_app_folder,
    ).run()
