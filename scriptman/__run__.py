from sys import argv

from scriptman.core.cli import CLI

if __name__ == "__main__":
    CLI.start_cli_instance(argv[1:])
