# Chrome Download Fallback Mechanism

## Overview

The Chrome download functionality in Scriptman now includes a robust, platform-agnostic fallback mechanism that automatically handles permission errors by using temporary directories when the primary download location is not accessible.

## Problem Solved

Previously, when the primary downloads directory (`scriptman/core/.downloads/.selenium/chrome/`) was not writable due to permission restrictions, the Chrome downloader would fail with a `PermissionError`. This was particularly problematic in:

- Corporate environments with restricted file system access
- Docker containers with limited permissions
- Systems with antivirus software blocking certain directories
- Network drives with write restrictions

## Solution

The new implementation provides:

1. **Automatic Fallback**: When the primary directory fails, it automatically switches to a temporary directory
2. **Platform Agnostic**: Uses Python's `tempfile` module for cross-platform compatibility
3. **Graceful Error Handling**: Provides clear logging about which directory is being used
4. **Cleanup Support**: Includes methods to clean up temporary directories when needed

## How It Works

### 1. Directory Selection Logic

```python
@property
def chrome_download_dir(self) -> Path:
    """Get the Chrome download directory with fallback to temp directory."""
    if self._chrome_download_dir is None:
        # Try primary downloads directory first
        primary_dir = Path(config.settings.downloads_dir, ".selenium", "chrome")
        try:
            primary_dir.mkdir(parents=True, exist_ok=True)
            # Test write permissions
            test_file = primary_dir / ".test_write"
            test_file.touch()
            test_file.unlink()
            self._chrome_download_dir = primary_dir
        except (PermissionError, OSError) as e:
            # Fallback to temp directory
            temp_base = Path(gettempdir())
            temp_dir = Path(mkdtemp(prefix="scriptman_chrome_", dir=temp_base))
            self._chrome_download_dir = temp_dir
            self._temp_download_dir = temp_dir
```

### 2. Download Process

The download process now includes multiple fallback points:

1. **Primary Directory Test**: Attempts to create and write to the primary directory
2. **Temporary Directory Creation**: If primary fails, creates a temporary directory
3. **Path Recalculation**: Updates all paths to use the new temporary location
4. **Error Recovery**: Cleans up partial downloads on failure

### 3. Platform Compatibility

The solution works across all major platforms:

- **Windows**: Uses `%TEMP%` environment variable
- **macOS**: Uses `/var/folders/...` or `/tmp`
- **Linux**: Uses `/tmp` or `/var/tmp`

## Usage

### Basic Usage

The fallback mechanism is completely transparent to the user:

```python
from scriptman.powers.selenium._chrome import ChromeDownloader

# This will automatically use fallback if needed
downloader = ChromeDownloader()
chrome_driver_path = downloader.download(139, "chromedriver")
```

### Monitoring Download Locations

You can check where files are being downloaded:

```python
# Get information about download directories
download_info = ChromeDownloader.get_download_info()
print(f"Primary directory writable: {download_info['primary_directory']['writable']}")
print(f"Temporary directories: {len(download_info['temp_directories'])}")
```

### Cleanup

The cleanup process now handles both primary and temporary directories:

```python
from scriptman.powers.cleanup import CleanUp

# This will clean up both primary and temporary Chrome directories
cleaner = CleanUp()
cleaner.selenium_cleanup()
```

## Configuration

### Environment Variables

The system respects these environment variables for temporary directory selection:

- **Windows**: `TEMP`, `TMP`
- **Unix-like**: `TMPDIR`, `TEMP`, `TMP`

### Logging

The system provides detailed logging about directory selection:

```
2025-06-23 14:58:47 | DEBUG    | Using primary download directory: /path/to/primary
2025-06-23 14:58:47 | WARNING  | Permission denied for primary directory /path/to/primary: [Errno 13] Permission denied. Using temporary directory: /tmp/scriptman_chrome_abc123
```

## Benefits

1. **Reliability**: Eliminates permission-related download failures
2. **Transparency**: Users don't need to change their code
3. **Cross-Platform**: Works consistently across Windows, macOS, and Linux
4. **Maintainability**: Clear separation of concerns and error handling
5. **Debugging**: Comprehensive logging for troubleshooting

## Testing

You can test the fallback mechanism using the provided test script:

```bash
python test_chrome_fallback.py
```

This will:
- Show current directory permissions
- Attempt a download (using fallback if needed)
- Display where files were actually downloaded

## Troubleshooting

### Common Issues

1. **Multiple Temporary Directories**: If you see multiple `scriptman_chrome_*` directories, they can be cleaned up using the cleanup functionality.

2. **Permission Errors Still Occurring**: Check if the temporary directory base location is writable:
   ```python
   import tempfile
   from pathlib import Path
   print(f"Temp directory base: {Path(tempfile.gettempdir())}")
   ```

3. **Disk Space Issues**: Temporary directories are created in the system temp location, which may have limited space.

### Debug Information

Use the `get_download_info()` method to diagnose issues:

```python
info = ChromeDownloader.get_download_info()
print(f"Download info: {info}")
```

## Future Enhancements

Potential improvements for future versions:

1. **Configurable Fallback Locations**: Allow users to specify custom fallback directories
2. **Automatic Cleanup**: Implement automatic cleanup of old temporary directories
3. **Compression**: Add support for compressed downloads to save space
4. **Caching**: Implement intelligent caching to avoid re-downloading existing files
