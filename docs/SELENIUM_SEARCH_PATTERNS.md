# Selenium Download Search Patterns

The `wait_for_downloads_to_finish` method now supports flexible search patterns using wildcards to find downloaded files.

## Overview

Instead of using an exact filename, you can now use search patterns that support wildcard matching. This is particularly useful when:
- File names contain timestamps or dynamic content
- You want to match files with similar naming patterns
- The exact filename is not known in advance

## Usage

### Basic Examples

```python
from scriptman.powers.selenium import SeleniumInstance

# Initialize selenium instance
selenium = SeleniumInstance()

# Wait for any file with "statement" in the name and .csv extension
file_path = selenium.wait_for_downloads_to_finish("*statement*.csv")

# Wait for any file starting with "report_" and ending with .pdf
file_path = selenium.wait_for_downloads_to_finish("report_*.pdf")

# Wait for any file ending with .xlsx
file_path = selenium.wait_for_downloads_to_finish("*.xlsx")

# Wait for any file (no pattern specified)
file_path = selenium.wait_for_downloads_to_finish()
```

### Pattern Examples

| Pattern | Matches | Example Files |
|---------|---------|---------------|
| `*statement*.csv` | Any CSV file with "statement" in the name | `bank_statement_2024.csv`, `statement_report.csv` |
| `report_*.pdf` | Any PDF file starting with "report_" | `report_2024.pdf`, `report_monthly.pdf` |
| `*.xlsx` | Any Excel file | `data.xlsx`, `sales_report.xlsx` |
| `export_*_*.csv` | CSV files starting with "export_" and containing two underscores | `export_data_2024.csv` |
| `*2024*.pdf` | Any PDF file with "2024" in the name | `report_2024.pdf`, `2024_summary.pdf` |

### How It Works

1. **Pattern Matching**: Uses Python's `fnmatch` module for wildcard pattern matching
2. **File Filtering**: Excludes temporary download files (`.crdownload`, `.part`, `.tmp`)
3. **Latest File**: Among matching files, selects the most recently modified file
4. **File Movement**: Moves the file to the configured downloads directory

### Wildcard Characters

- `*` - Matches any sequence of characters
- `?` - Matches any single character
- `[seq]` - Matches any character in the sequence
- `[!seq]` - Matches any character not in the sequence

### Best Practices

1. **Be Specific**: Use patterns that are specific enough to avoid matching unwanted files
2. **Include Extensions**: Always include file extensions in your patterns
3. **Test Patterns**: Test your patterns with sample filenames before using in production
4. **Handle Timeouts**: Set appropriate timeout values for your use case

### Error Handling

The method will raise a `TimeoutException` if no matching files are found within the specified timeout period. Make sure your pattern is correct and the file is actually being downloaded.

### Migration from Old API

If you were using the old `file_name` parameter, you can easily migrate:

```python
# Old way
file_path = selenium.wait_for_downloads_to_finish("report.pdf")

# New way (equivalent)
file_path = selenium.wait_for_downloads_to_finish("report.pdf")

# New way (more flexible)
file_path = selenium.wait_for_downloads_to_finish("report_*.pdf")
```
