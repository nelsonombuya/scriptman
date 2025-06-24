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

### File Deletion Behavior

Downloaded files can be automatically deleted when the SeleniumInstance is garbage collected. You control this behavior using the `mark_for_deletion` parameter:

```python
# Mark this file for deletion (default)
file_path = selenium.wait_for_downloads_to_finish("*statement*.csv", mark_for_deletion=True)

# Keep this file (won't be deleted)
file_path = selenium.wait_for_downloads_to_finish("*statement*.csv", mark_for_deletion=False)
```

#### Example Usage

```python
# Initialize selenium instance
selenium = SeleniumInstance()

# This file will be deleted when selenium is garbage collected
temp_file = selenium.wait_for_downloads_to_finish("*temp*.csv", mark_for_deletion=True)

# This file will be kept
important_file = selenium.wait_for_downloads_to_finish("*important*.pdf", mark_for_deletion=False)

# Default behavior (mark_for_deletion=True)
file_path = selenium.wait_for_downloads_to_finish("*statement*.csv")
```

### Case Sensitivity

By default, pattern matching is case sensitive. You can control this behavior with the `case_sensitive` parameter:

```python
# Case sensitive (default) - only matches exact case
file_path = selenium.wait_for_downloads_to_finish("*statement*.csv", case_sensitive=True)
# Matches: bank_statement_2024.csv
# Does NOT match: Bank_Statement_2024.csv

# Case insensitive - matches regardless of case
file_path = selenium.wait_for_downloads_to_finish("*statement*.csv", case_sensitive=False)
# Matches: bank_statement_2024.csv, Bank_Statement_2024.csv, STATEMENT_2024.csv
```

### Pattern Examples

| Pattern | Case Sensitive | Matches | Example Files |
|---------|----------------|---------|---------------|
| `*statement*.csv` | True | Any CSV file with "statement" in the name | `bank_statement_2024.csv`, `statement_report.csv` |
| `*statement*.csv` | False | Any CSV file with "statement" in the name (any case) | `bank_statement_2024.csv`, `Bank_Statement_2024.csv` |
| `report_*.pdf` | True | Any PDF file starting with "report_" | `report_2024.pdf`, `report_monthly.pdf` |
| `report_*.pdf` | False | Any PDF file starting with "report_" (any case) | `report_2024.pdf`, `Report_2024.pdf` |
| `*.xlsx` | True/False | Any Excel file | `data.xlsx`, `sales_report.xlsx` |
| `export_*_*.csv` | True | CSV files starting with "export_" and containing two underscores | `export_data_2024.csv` |
| `*2024*.pdf` | True | Any PDF file with "2024" in the name | `report_2024.pdf`, `2024_summary.pdf` |

### How It Works

1. **Pattern Matching**: Uses Python's `fnmatch` module for wildcard pattern matching
2. **Case Sensitivity**: Controlled by the `case_sensitive` parameter (default: True)
3. **File Filtering**: Excludes temporary download files (`.crdownload`, `.part`, `.tmp`)
4. **Latest File**: Among matching files, selects the most recently modified file
5. **File Movement**: Moves the file to the configured downloads directory
6. **Deletion Tracking**: Optionally marks files for automatic deletion

### Wildcard Characters

- `*` - Matches any sequence of characters
- `?` - Matches any single character
- `[seq]` - Matches any character in the sequence
- `[!seq]` - Matches any character not in the sequence

### Best Practices

1. **Be Specific**: Use patterns that are specific enough to avoid matching unwanted files
2. **Include Extensions**: Always include file extensions in your patterns
3. **Consider Case**: Decide whether case sensitivity matters for your use case
4. **Manage Deletion**: Use `mark_for_deletion=False` for files you want to keep
5. **Test Patterns**: Test your patterns with sample filenames before using in production
6. **Handle Timeouts**: Set appropriate timeout values for your use case

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

# New way (case insensitive)
file_path = selenium.wait_for_downloads_to_finish("report_*.pdf", case_sensitive=False)

# New way (keep file)
file_path = selenium.wait_for_downloads_to_finish("report_*.pdf", mark_for_deletion=False)
```

**Note**: The `remove_downloaded_files` constructor parameter has been removed. File deletion is now controlled entirely through the `mark_for_deletion` parameter in the `wait_for_downloads_to_finish` method.
