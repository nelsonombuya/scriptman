import getpass
import os
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

    def __init__(self, package_name, version, use_dotenv, del_app_folder):
        self.del_app_folder = del_app_folder
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

    def delete_dist_folder(self):
        """
        Delete the 'dist' folder in the Script Manager directory if it exists.
        """
        dist_folder = os.path.join(self.dir, "dist")
        if os.path.exists(dist_folder):
            shutil.rmtree(dist_folder)
            print(f"Deleted '{dist_folder}' folder.")

    def delete_egg_info_folder(self):
        """
        Delete the '{package_name}.egg-info' folder in the Script Manager
        directory if it exists.
        """
        egg_info_dir = os.path.join(self.dir, f"{self.package_name}.egg-info")
        if os.path.exists(egg_info_dir):
            shutil.rmtree(egg_info_dir)
            print(f"Deleted '{egg_info_dir}' folder.")

    def delete_local_app_folder(self):
        """
        Delete the app folder in the Script Manager that's used for testing
        if it exists.
        """
        app_dir = os.path.join(self.dir, "app")
        if os.path.exists(app_dir) and self.del_app_folder.lower() == "true":
            shutil.rmtree(app_dir)
            print(f"Deleted '{app_dir}' folder.")

    def update_version(self):
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

    def create_sm_bat_file(self):
        bat_file_path = r"scriptman\_scriptman.bat"
        python_file_path = r"scriptman\_batch.py"
        with open(bat_file_path, "r") as bat_file:
            content = bat_file.read()
            python_variable = f'BATCH_FILE = r"""{content}"""\n'
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
        self.create_sm_bat_file()
        self.delete_dist_folder()
        self.update_version()
        self.run_build()
        self.upload_to_twine()
        print("Package publishing completed.")


if __name__ == "__main__":
    package_name, version, use_dotenv, del_app_folder = sys.argv[1:]
    PackagePublishHelper(
        package_name,
        version,
        use_dotenv,
        del_app_folder,
    ).run()
