import os
import shutil
import subprocess
import sys


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

    Usage:
        Create an instance of PackagePublishHelper with the package name and
        new version, then call the `run` method to execute the package
        publishing process.

    Example:
        >>> helper = PackagePublishHelper("my_package", "1.0.2")
        >>> helper.run()
    """

    def __init__(self, package_name, version):
        self.dir = os.path.dirname(__file__)
        self.package_name = package_name
        self.version = version

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
        subprocess.run(["python", "-m", "build"])
        print("Built distribution packages.")

    def upload_to_twine(self):
        """
        Upload distribution packages to Twine using 'twine upload'.
        """
        subprocess.run(["twine", "upload", "dist/*"])
        print("Uploaded distribution packages to Twine.")

    def run(self):
        """
        Run the package publishing process, including deleting folders,
        updating version, building distribution packages, and uploading to
        Twine.
        """
        self.delete_dist_folder()
        self.delete_egg_info_folder()
        self.update_version()
        self.run_build()
        self.upload_to_twine()
        print("Package publishing completed.")


if __name__ == "__main__":
    package_name, version = sys.argv[1:]
    PackagePublishHelper(package_name, version).run()
