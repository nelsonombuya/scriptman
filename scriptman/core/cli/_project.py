import subprocess
import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path

from loguru import logger

from scriptman.core.cli._parser import BaseParser
from scriptman.core.config import config


class ProjectSubParser(BaseParser):
    """
    Handles project management operations like updating code, managing dependencies,
    and other project-related tasks.
    """

    def __init__(self, sub_parser: "_SubParsersAction[ArgumentParser]") -> None:
        """
        🚀 Initializes a ProjectSubParser instance with an ArgumentParser.

        Args:
            sub_parser: ArgumentParser instance to use for parsing CLI arguments.
        """
        self.parser: ArgumentParser = sub_parser.add_parser(
            "project", help="Manage the local project where scriptman is installed."
        )

        # Initialize sub-commands
        self.init_arguments()

    @property
    def command(self) -> str:
        """
        ⚙ Get the name of the command being parsed.

        Returns:
            str: The name of the command being parsed.
        """
        return "project"

    def init_arguments(self) -> None:
        """
        ⚙ Add arguments for project management operations.

        This function adds the following arguments to the CLI parser:

        - `--update-code`: Update project code using git pull.
        - `--ignore-local-changes`: When updating code, stash local changes, pull, then
            restore.
        - `--update-deps`: Update project dependencies based on the detected package
            manager.
        - `--force`: Force operations that might otherwise fail due to warnings.
        """
        self.parser.add_argument(
            "-c",
            "--update-code",
            action="store_true",
            help="Update project code using git pull.",
        )

        self.parser.add_argument(
            "-i",
            "--ignore-local-changes",
            action="store_true",
            help="When updating code, stash local changes, pull, then restore stash.",
        )

        self.parser.add_argument(
            "-d",
            "--update-deps",
            action="store_true",
            help="Update project dependencies based on detected package manager.",
        )

        self.parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="Force operations that might otherwise fail due to warnings.",
        )

        self.parser.add_argument(
            "-s",
            "--status",
            action="store_true",
            help="Show project status including git status and dependency info.",
        )

    def process(self, args: Namespace) -> int:
        """
        ⚙ Process parsed CLI arguments for the 'project' command.

        This function takes the parsed CLI arguments as a Namespace object and performs
        project management operations based on the specified options.

        Args:
            args (Namespace): Parsed CLI arguments containing project management options.

        Returns:
            int: Exit code (0 for success, non-zero for failure)
        """
        if not any([args.update_code, args.update_deps, args.status]):
            # If no specific action provided, show help
            self.parser.print_help()
            return 0

        # Check if we're in a git repository
        is_git_repo = self._is_git_repository()

        if args.status:
            return self._show_project_status(is_git_repo)

        # Handle project code update
        if args.update_code and is_git_repo:
            success = self._update_project_code(args.ignore_local_changes, args.force)
            if not success:
                return 1

        # Handle dependency update
        if args.update_deps:
            success = self._update_dependencies(args.force)
            if not success:
                return 1

        return 0

    def _is_git_repository(self) -> bool:
        """Check if current directory is a git repository."""
        try:
            subprocess.run(
                ["git", "rev-parse", "--is-inside-work-tree"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                cwd=config.cwd,
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def _show_project_status(self, is_git_repo: bool) -> int:
        """Show project status including git status and dependency info."""
        logger.info("📊 Project Status")
        logger.info(f"🏠 Project directory: {config.cwd}")

        # Git status
        if is_git_repo:
            try:
                logger.info("🌿 Git Repository Status:")
                result = subprocess.run(
                    ["git", "status", "--short"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                    cwd=config.cwd,
                )
                if result.stdout.strip():
                    for line in result.stdout.strip().split("\n"):
                        logger.info(f"  {line}")
                else:
                    logger.info("  Clean working directory")

                # Get current branch
                result = subprocess.run(
                    ["git", "branch", "--show-current"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                    cwd=config.cwd,
                )
                branch = result.stdout.strip()
                logger.info(f"  Current branch: {branch}")
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Error getting git status: {e}")
                return 1
        else:
            logger.info("🚫 Not a git repository")

        # Dependency management info
        if Path(config.cwd / "pyproject.toml").exists():
            logger.info("📦 Using Poetry for dependency management")
            try:
                result = subprocess.run(
                    ["poetry", "show", "--tree"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=config.cwd,
                )
                if result.returncode == 0:
                    logger.debug("  Poetry dependencies installed")
                else:
                    logger.warning(
                        "⚠ Poetry dependencies not installed or Poetry not available"
                    )
            except FileNotFoundError:
                logger.warning("⚠ Poetry not found in PATH")
        elif Path(config.cwd / "requirements.txt").exists():
            logger.info("📦 Using requirements.txt for dependency management")
            try:
                with open(config.cwd / "requirements.txt", "r") as f:
                    deps = len(f.readlines())
                logger.debug(f"  {deps} packages listed in requirements.txt")
            except Exception as e:
                logger.error(f"❌ Error reading requirements.txt: {e}")
        else:
            logger.info("📦 No recognized dependency management file found")

        return 0

    def _update_project_code(self, ignore_local_changes: bool, force: bool) -> bool:
        """Update project code using git pull."""
        logger.info("🔄 Updating project code...")

        # Check for uncommitted changes
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            cwd=config.cwd,
        )

        has_local_changes = bool(result.stdout.strip())

        if has_local_changes:
            if not ignore_local_changes and not force:
                logger.warning(
                    "⚠️ Uncommitted local changes detected. "
                    "Use --ignore-local-changes to stash them temporarily "
                    "or --force to proceed anyway."
                )
                return False

            if ignore_local_changes:
                logger.info("📦 Stashing local changes...")
                stash_result = subprocess.run(
                    ["git", "stash", "push", "-m", "scriptman auto-stash before pull"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=config.cwd,
                )
                if stash_result.returncode != 0:
                    logger.error(f"❌ Failed to stash changes: {stash_result.stderr}")
                    if stash_result.stdout:
                        logger.debug(f"Stash output: {stash_result.stdout}")
                    return False
                logger.info(stash_result.stdout.strip())

        # Pull latest changes
        try:
            logger.info("⬇️ Pulling latest changes...")
            pull_result = subprocess.run(
                ["git", "pull"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
                cwd=config.cwd,
            )
            if pull_result.stdout:
                logger.info(pull_result.stdout.strip())

            # Check if there was actually any update
            if "Already up to date" in pull_result.stdout:
                logger.info("✓ Repository already up to date")
            elif "Updating" in pull_result.stdout:
                logger.info("✓ Repository updated successfully")

            # Check for non-fatal warnings
            if pull_result.stderr and "fatal" not in pull_result.stderr.lower():
                logger.warning(f"⚠️ Git pull warnings: {pull_result.stderr}")

        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Git pull failed: {e.stderr}")
            if e.stdout:
                logger.debug(f"Command output: {e.stdout}")

            # If we stashed changes, try to restore them even if pull failed
            if ignore_local_changes and has_local_changes:
                logger.info("📦 Restoring stashed changes...")
                subprocess.run(
                    ["git", "stash", "pop"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=config.cwd,
                )

            return False

        # Restore stashed changes if needed
        if ignore_local_changes and has_local_changes:
            logger.info("📦 Restoring stashed changes...")
            pop_result = subprocess.run(
                ["git", "stash", "pop"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=config.cwd,
            )
            if pop_result.returncode != 0:
                logger.error(f"❌ Failed to restore stashed changes: {pop_result.stderr}")
                if pop_result.stdout:
                    logger.debug(f"Command output: {pop_result.stdout}")
                logger.warning(
                    "⚠️ Your changes are stored in the git stash. "
                    "Use 'git stash apply' to recover them."
                )
                return False
            logger.info(pop_result.stdout.strip())

        logger.success("✅ Project code updated successfully")
        return True

    def _update_dependencies(self, force: bool) -> bool:
        """Update project dependencies based on detected package manager."""
        logger.info("📦 Updating project dependencies...")

        # Detect package manager
        is_poetry = Path(config.cwd / "pyproject.toml").exists()
        is_pip = Path(config.cwd / "requirements.txt").exists()

        if is_poetry:
            logger.info("🧩 Poetry detected, updating dependencies...")
            try:
                # Lock dependencies first
                logger.info("📝 Updating poetry.lock file...")
                lock_result = subprocess.run(
                    ["poetry", "lock"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                    cwd=config.cwd,
                )
                if lock_result.stdout:
                    logger.info(lock_result.stdout.strip())

                # Install dependencies
                logger.info("📚 Installing dependencies...")
                install_result = subprocess.run(
                    ["poetry", "install"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                    cwd=config.cwd,
                )
                if install_result.stdout:
                    logger.info(install_result.stdout.strip())

                # Update dependencies to latest versions
                logger.info("🔄 Updating dependencies to latest versions...")
                update_result = subprocess.run(
                    ["poetry", "update"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                    cwd=config.cwd,
                )
                if update_result.stdout:
                    logger.info(update_result.stdout.strip())

                # Check for non-error output in stderr (warnings)
                for result in [lock_result, install_result, update_result]:
                    if result.stderr and "error" not in result.stderr.lower():
                        logger.warning(f"⚠️ Poetry warnings: {result.stderr}")

                logger.success("✅ Dependencies updated successfully with Poetry")
                return True
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Poetry update failed: {e.stderr}")
                if e.stdout:
                    logger.debug(f"Command output: {e.stdout}")
                return False
            except FileNotFoundError:
                logger.error(
                    "❌ Poetry not found. "
                    "Please install Poetry or use pip with requirements.txt."
                )
                return False

        elif is_pip:
            logger.info("🧩 requirements.txt detected, updating dependencies...")
            try:
                # Get current Python executable
                python_exe = sys.executable

                # Check if we're in a virtual environment
                in_venv = hasattr(sys, "real_prefix") or (
                    hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
                )
                if not in_venv and not force:
                    logger.warning(
                        "⚠️ Not running in a virtual environment. "
                        "It's recommended to update dependencies in a virtual environment"
                    )
                    logger.warning("⚠️ Use --force to update dependencies anyway.")
                    return False

                # Update pip itself first
                pip_update_result = subprocess.run(
                    [python_exe, "-m", "pip", "install", "--upgrade", "pip"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                    cwd=config.cwd,
                )
                logger.info("Updated pip:")
                logger.info(pip_update_result.stdout.strip())

                # Update packages from requirements.txt
                logger.info("Updating packages from requirements.txt...")
                update_result = subprocess.run(
                    [
                        python_exe,
                        "-m",
                        "pip",
                        "install",
                        "--upgrade",
                        "-r",
                        "requirements.txt",
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                    cwd=config.cwd,
                )

                # Log the output to show what happened
                if update_result.stdout:
                    logger.info(update_result.stdout.strip())

                # Check for warnings in stderr but don't fail
                if update_result.stderr:
                    if "error" in update_result.stderr.lower():
                        logger.error(
                            f"❌ pip update encountered errors: {update_result.stderr}"
                        )
                        return False
                    else:
                        # Just warnings, not fatal
                        logger.warning(f"⚠️ pip update warnings: {update_result.stderr}")

                logger.success("✅ Dependencies updated successfully with pip")
                return True
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ pip update failed: {e.stderr}")
                if e.stdout:
                    logger.debug(f"Command output: {e.stdout}")
                return False
        else:
            logger.warning(
                "⚠️ No recognized dependency management file found "
                "(pyproject.toml or requirements.txt)"
            )
            return False
