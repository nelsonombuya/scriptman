# Scriptman

Scriptman is a batteries-included Python automation toolkit that helps you build
web automation, ETL pipelines, and scheduled workflows with minimal ceremony.

## Features

- 🚀 **Selenium Automation** – Cross-platform browser automation with intelligent download handling
- 📊 **ETL Operations** – Pandas-powered Extract/Transform/Load helpers for CSV, JSON, and databases
- 🧠 **Task Manager & Services** – Cooperative services, multithreaded execution, and retry utilities
- ⏰ **Scheduling** – Interval, daily, and one-off triggers with decorator-friendly APIs
- 🗄️ **Database Support** – SQLAlchemy and pyodbc integrations out of the box
- ⚙️ **Configuration** – Flexible TOML configuration with sensible defaults
- 📚 **Documentation** – In-depth guides under [`scriptman/docs`](scriptman/docs)

## Quick Start

### Installation

```bash
pip install scriptman
```

### Hello Scriptman

```python
from datetime import timedelta

from scriptman import TaskManager, scheduler

manager = TaskManager()

# Register a cooperative service loop
def send_heartbeat() -> None: ...  # your implementation

@manager.service(name="heartbeat", autostart=True)
def heartbeat(ctx):
    while not ctx.should_stop:
        send_heartbeat()
        if not ctx.sleep(60):  # exit early if shutdown requested
            break

# Schedule a periodic task
@scheduler.schedule(trigger=scheduler.IntervalTrigger(timedelta(minutes=30)))
def sync_remote_data():
    ...  # your sync routine

if __name__ == "__main__":
    manager.start_service("heartbeat")
```

## Task Manager, Services & Scheduler

```python
from datetime import time

from scriptman import TaskManager, scheduler

manager = TaskManager()

@manager.service(name="report-generator", autostart=True)
def report_service(ctx):
    while not ctx.should_stop:
        generate_incremental_report()
        if not ctx.sleep(300):
            break

@manager.service(name="summary", autostart=False)
def summary_service(ctx):
    generate_summary()

@scheduler.schedule(trigger=scheduler.TimeOfDayTrigger(at=time(hour=21)))
def nightly_rollup() -> None:
    finalize_daily_metrics()

manager.start_service("report-generator")
```

The scheduler proxy (`scriptman.scheduler`) is always available, even before a
`TaskManager` is explicitly constructed, and the service proxy
(`scriptman.service_manager`) lets you introspect currently registered services.

## Selenium & Downloads

```python
from scriptman.powers.selenium import SeleniumInstance

selenium = SeleniumInstance()
selenium.driver.get("https://example.com")
selenium.interact_with_element("//button[@id='download']", mode="click")
downloaded_file = selenium.wait_for_downloads_to_finish("report.pdf")
print(f"Downloaded: {downloaded_file}")
```

Scriptman defaults to your system Downloads directory, automatically monitoring
Chrome’s default location and relocating finished downloads. Override it in
configuration if you need a custom path:

```toml
# scriptman.toml
[scriptman]
downloads_dir = "/path/to/custom/downloads"
```

## ETL & Data Pipelines

```python
from scriptman.powers.etl import ETL

etl = (
    ETL.from_db(prod_db, "SELECT * FROM sales")
    .transform(add_calculated_fields)
    .to_db(warehouse_db, "sales_fact", method="upsert")
)
```

Full ETL documentation—including architecture, examples, and API reference—lives
under [`scriptman/docs/powers/etl`](scriptman/docs/powers/etl).

## Configuration Snapshot

```toml
# scriptman.toml
[scriptman]
log_level = "INFO"
downloads_dir = "~/Downloads"

[scriptman.selenium]
headless = true
local_mode = true

[scriptman.tasks]
retries = 3
task_timeout = 30
```

## Documentation

Detailed guides and API references live in the [`scriptman/docs`](scriptman/docs)
directory. Start with the module READMEs (e.g. the ETL quick-start) and keep an
eye out for upcoming documentation on tasks, services, scheduler, Selenium, and
database helpers.

## License

This project is licensed under the MIT License—see the [LICENSE](LICENSE) file
for details.
