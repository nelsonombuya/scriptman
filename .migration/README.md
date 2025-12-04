# 📋 Scriptman v3 Migration Plans

This folder contains detailed implementation plans for migrating/implementing Scriptman v3 modules.

## 📁 Contents

| File                  | Module        | Status        | Description                                           |
| --------------------- | ------------- | ------------- | ----------------------------------------------------- |
| `01-serialization.md` | Serialization | ✅ Complete    | Type conversion utilities                             |
| `02-types.md`         | Types         | ✅ Complete    | Type aliases and sync/async utilities (simplified)    |
| `03-config.md`        | Config        | 🚧 In Progress | Configuration management with optional format readers |
| `04-internal-log.md`  | Internal Log  | ✅ Complete    | Safe internal logging + formatting presets            |
| `05-cache.md`         | Cache         | ✅ Complete    | SQLite-backed caching with TTL, tags, LRU             |
| `06-retry.md`         | Retry         | 💡 Future      | Retry decorator with backoff                          |
| `07-queue.md`         | Queue         | 💡 Future      | SQLite-backed message queue                           |
| `08-scheduler.md`     | Scheduler     | 💡 Future      | Task scheduling system                                |
| `09-tasks.md`         | Tasks         | 📋 Planned     | Thread-based task execution                           |

## 🎯 Implementation Order

The files are numbered in the recommended implementation order:

1. **Serialization** — Basic utilities with no dependencies
2. **Types Simplification** — Type definitions for decorators (`@overload` pattern)
3. **Config** — Configuration system (foundation for everything)
   - JSON default (no deps)
   - TOML/YAML/dotenv as optional extras
4. **Internal Log** — Safe internal logging + formatting presets
5. **Cache** — Persistent caching with TTL, tags, LRU (uses database, config, types)
6. **Retry** — Retry decorator (uses types, observe)
7. **Queue** — Message queue (uses database patterns from cache)
8. **Scheduler** — Task scheduling (uses queue, tasks)
9. **Tasks** — High-level task execution (uses types, observe, config)

## 📦 Dependency Philosophy

Scriptman follows **minimal core dependencies**:

| Core (Always Installed) | Optional Extras                         |
| ----------------------- | --------------------------------------- |
| `loguru`                | `scriptman[toml]` — TOML config support |
| `pydantic`              | `scriptman[yaml]` — YAML config support |
|                         | `scriptman[dotenv]` — .env file support |
|                         | `scriptman[api]` — FastAPI integration  |

**Default Experience:** JSON config files (stdlib, no extra deps needed)

## 📊 Plan Status Legend

| Status        | Meaning                                    |
| ------------- | ------------------------------------------ |
| ✅ Complete    | Implemented, tested, documented            |
| 🚧 In Progress | Currently being worked on                  |
| 📋 Planned     | Plan is complete, ready for implementation |
| 💡 Future      | Placeholder for future planning            |

## 🎯 How to Use These Plans

1. Open a new Cursor thread/conversation
2. Paste the contents of the relevant `.md` file
3. Follow the step-by-step implementation guide
4. Update `FEATURE_GUIDE.md` after completing each module

## 📝 Notes

- Plans are designed to be self-contained with all context needed
- Each plan references the current codebase structure
- Follow the implementation order within each plan
- Plans include testing checklists and compliance verification

## 🔗 Related Documents

- **Feature Guide:** `FEATURE_GUIDE.md` — Living inventory of all features
- **Development Rules:** `.cursor/rules/scriptman.mdc` — Coding standards
