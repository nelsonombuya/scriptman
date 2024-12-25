# ScriptMan

ScriptMan is a flexible script management CLI tool that allows you to run, manage, and configure scripts with ease.

## Installation

### Using Poetry

1. Install Poetry (if not already installed):
```bash
pip install poetry
```

2. Clone the repository:
```bash
git clone https://github.com/nelsonombuya/scriptman.git
cd scriptman
```

3. Install dependencies:
```bash
poetry install
```

### Using pip

```bash
pip install .
```

## Usage

### Basic Usage

Run all scripts:
```bash
scriptman
```

Run specific scripts:
```bash
scriptman script1.py script2.py
```

### Advanced Options

- `-d, --debug`: Enable debugging mode
- `-q, --quick`: Enable quick mode (skip updates)
- `-f, --force`: Force scripts to run
- `-c, --custom`: Run custom scripts
- `-dl, --disable-logging`: Disable logging
- `-cl, --clear-lock`: Clear lock files
- `-i, --ignore`: Ignore specific scripts

## Development

### Running Tests

```bash
poetry run pytest
```

### Code Formatting

```bash
poetry run black .
poetry run isort .
```

### Type Checking

```bash
poetry run mypy scriptman
```
