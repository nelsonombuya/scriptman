# ScriptMan - A Python Package for Script Management

ScriptMan is a Python package that provides a comprehensive set of tools and utilities for managing Python scripts. Whether you're working with data, databases, command-line interfaces, web automation, or just need better script organization, ScriptMan has you covered.

## Installation

You can install ScriptMan using pip:

```bash
pip install scriptman
```

## Usage

```python
import scriptman

# Example: Run a script
scriptman.ScriptsHandler().run_script('my_script.py')
```

## Features

### CleanUpHandler

- Provides cleanup functionalities for scripts.

### CLIHandler

- Handles command-line interface interactions.

### CSVHandler

- Offers utilities for working with CSV files.

### DatabaseHandler

- Provides database interaction capabilities.

### DirectoryHandler

- Manages directories and file operations.

### ETLHandler

- Offers tools for Extract, Transform, Load (ETL) processes.

### LogHandler

- Handles logging with different log levels.

### LogLevel

- Enum for different log levels.

### ScriptsHandler

- Manages the execution of scripts.

### SeleniumHandler

- Provides tools for web automation using Selenium.

### SeleniumInteraction

- Enum for Selenium-based interactions.

### Settings

- Accesses and manages package settings.

## Initialization

To set up ScriptMan for your project, you should call the following method from the `Settings` class upon start:

```python
from scriptman import Settings

Settings.init(
    app_dir='your_project_directory',
    logging=True,  # Enable logging (default is True)
    debugging=False,  # Enable debugging mode (default is False)
)
```

Calling this method will set up ScriptMan's app files in your project directory under the app folder. It will create various folders:

- `downloads`: Used for downloads (e.g., Downloads made by Selenium).
- `helpers`: Used for any extra modules you want to reuse in your code (e.g., API Classes).
- `scripts`: Where your scripts should reside.
- `Logs`: Where the log files will be saved.

## Documentation

For detailed documentation and examples, please refer to the [package documentation](https://github.com/nelsonombuya/scriptman/blob/main/docs/README.md).

## Example

```python
# Import ScriptMan modules
import scriptman

# Create a ScriptsHandler instance
script_handler = scriptman.ScriptsHandler()

# Run a Python script
script_handler.run_script('my_script.py')
```

## Contributing

We welcome contributions! Please feel free to submit issues and pull requests to help improve this package.

## License

This package is distributed under the [MIT License](https://opensource.org/licenses/MIT).

