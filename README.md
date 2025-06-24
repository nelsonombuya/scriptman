# Scriptman

A powerful Python automation framework for web scraping, data processing, and task orchestration.

## Features

- 🚀 **Selenium Automation**: Cross-platform browser automation with intelligent fallback mechanisms
- 📊 **ETL Operations**: Extract, Transform, Load data processing with pandas integration
- 🗄️ **Database Support**: Multi-database support with SQLAlchemy and pyodbc
- ⏰ **Task Scheduling**: Advanced task scheduling and resource management
- 🔄 **Retry Mechanisms**: Robust retry logic with exponential backoff
- 🧹 **Cleanup Utilities**: Automatic cleanup of temporary files and caches
- ⚙️ **Configuration Management**: Flexible configuration with TOML support

## Quick Start

### Installation

```bash
pip install scriptman
```

### Basic Usage

```python
from scriptman.powers.selenium import SeleniumInstance

# Initialize Selenium with automatic downloads directory
selenium = SeleniumInstance()

# Navigate to a website
selenium.driver.get("https://example.com")

# Download a file (will be saved to your system's Downloads folder)
# ... download logic here ...

# Wait for download to complete
downloaded_file = selenium.wait_for_downloads_to_finish()
print(f"File downloaded to: {downloaded_file}")
```

## Downloads Directory

Scriptman automatically uses your system's default Downloads directory:

- **Windows**: `C:\Users\<username>\Downloads` (with OneDrive fallback)
- **macOS**: `/Users/<username>/Downloads`
- **Linux**: `/home/<username>/Downloads`

### Smart Download Handling

Scriptman uses an intelligent download mechanism that:

1. **Lets Chrome use its default download directory** (usually your Downloads folder)
2. **Monitors for completed downloads** in Chrome's default location
3. **Automatically moves files** to your configured downloads directory
4. **Handles filename conflicts** by adding counters to duplicate names

This approach prevents download issues that can occur when forcing Chrome to use a specific download directory.

### Configuration

You can customize the downloads directory in your configuration:

```toml
# scriptman.toml
[scriptman]
downloads_dir = "/path/to/custom/downloads"
```

### File Organization

- **Selenium Downloads**: Files downloaded through Selenium are saved to the configured directory
- **Chrome/Selenium Files**: Browser executables are stored in `.selenium/chrome/` subdirectory
- **Temporary Files**: Fallback to system temp directory if Downloads is not writable

## Selenium Features

### Cross-Platform Support

Scriptman's Selenium implementation includes:

- **Automatic Browser Management**: Downloads and manages Chrome/ChromeDriver
- **Fallback Mechanisms**: Graceful handling of permission issues
- **Headless Mode**: Optimized for server environments
- **Download Monitoring**: Automatic detection and relocation of completed downloads

### Example: Web Scraping

```python
from scriptman.powers.selenium import SeleniumInstance

selenium = SeleniumInstance()

# Navigate and interact
selenium.driver.get("https://example.com")
selenium.interact_with_element("//button[@id='download']", mode="click")

# Wait for download and get file path
file_path = selenium.wait_for_downloads_to_finish("report.pdf")
print(f"Downloaded: {file_path}")
```

### Download Process Flow

1. **Chrome downloads** to its default directory (usually Downloads)
2. **Scriptman monitors** the default directory for new files
3. **File detection** occurs when download completes
4. **Automatic move** to configured directory
5. **Filename conflict resolution** if needed
6. **Return final path** in configured directory

## ETL Operations

```python
from scriptman.powers.etl import ETL

# Load data from various sources
data = ETL.from_csv("data.csv")
data = ETL.from_json("data.json")
data = ETL.from_db(database_handler, "SELECT * FROM table")

# Transform data
transformed = data.filter(lambda x: x['status'] == 'active')
transformed = transformed.to_snake_case()

# Save results
transformed.to_csv("output.csv")
transformed.to_db(database_handler, "output_table")
```

## Configuration

Scriptman uses TOML configuration files:

```toml
# scriptman.toml
[scriptman]
# Downloads directory (defaults to system Downloads folder)
downloads_dir = "~/Downloads"

# Selenium settings
selenium_optimizations = true
selenium_headless = true
selenium_local_mode = true

# Logging
log_level = "INFO"

# Task settings
concurrent = true
retries = 3
task_timeout = 30
```

## Advanced Features

### Task Scheduling

```python
from scriptman.powers.scheduler import TaskScheduler

scheduler = TaskScheduler()

# Schedule a daily task
scheduler.add_daily_task(
    "daily_report",
    task_function,
    hour=9,
    minute=0
)

# Schedule a periodic task
scheduler.add_periodic_task(
    "data_sync",
    sync_function,
    interval_minutes=30
)
```

### Database Operations

```python
from scriptman.powers.database import DatabaseHandler

# Connect to database
db = DatabaseHandler(
    connection_string="sqlite:///data.db"
)

# Execute queries
results = db.execute_read_query("SELECT * FROM users")
db.execute_write_query("INSERT INTO logs VALUES (?)", ["log_entry"])
```

### Cleanup Utilities

```python
from scriptman.powers.cleanup import CleanUp

cleaner = CleanUp()

# Clean up various resources
cleaner.cleanup()  # General cleanup
cleaner.selenium_cleanup()  # Selenium downloads
cleaner.diskcache_cleanup()  # Cache files
```

## Development

### Installation for Development

```bash
git clone <repository>
cd scriptman
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest
```

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here]
