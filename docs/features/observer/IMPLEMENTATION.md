# Observer Implementation Guide

> **Feature:** Context-aware observability system for Python automation workflows

**Status:** 🚧 In Development (v3.0)  
**Last Updated:** February 5, 2026  
**Implementation Phase:** Foundation

---

## 📋 Table of Contents

1. [Feature Overview](#feature-overview)
2. [Goals & Acceptance Criteria](#goals--acceptance-criteria)
3. [Architecture](#architecture)
4. [Implementation Plan](#implementation-plan)
5. [Code Examples](#code-examples)
6. [Testing Requirements](#testing-requirements)
7. [Decision Log](#decision-log)
8. [Progress Tracker](#progress-tracker)
9. [Integration Notes](#integration-notes)
10. [Performance Targets](#performance-targets)
11. [Post-Implementation Review](#post-implementation-review)

---

## 🎯 Feature Overview

### What We're Building

A **function-level observability system** that:
- Tracks execution flow across nested function calls
- Propagates context (tracked properties) automatically down the call stack
- Provides beautiful console output (ASCII tree rendering)
- Stores events in SQLite for historical analysis
- Doubles as a testing tool (trace mode)

### The Problem It Solves

**Before Observer:**
```python
def process_invoice(invoice_id: str):
    print(f"[{datetime.now()}] Processing invoice {invoice_id}")
    validate_invoice(invoice_id)
    print(f"[{datetime.now()}] Validated invoice {invoice_id}")
    send_to_tax(invoice_id)
    print(f"[{datetime.now()}] Sent invoice {invoice_id} to tax system")

def validate_invoice(invoice_id: str):
    print(f"[{datetime.now()}] Validating invoice {invoice_id}")
    # Validation logic
    print(f"[{datetime.now()}] Invoice {invoice_id} valid")

def send_to_tax(invoice_id: str):
    print(f"[{datetime.now()}] Sending invoice {invoice_id} to tax")
    # Tax logic
    print(f"[{datetime.now()}] Invoice {invoice_id} sent")
```

**Problems:**
- ❌ Manual `invoice_id` passing everywhere
- ❌ Timestamp formatting repeated
- ❌ No nesting visualization
- ❌ No storage for historical analysis
- ❌ Hard to test flow

---

**After Observer:**
```python
@observe(track="invoice_id")
def process_invoice(invoice_id: str):
    observe.info("Processing invoice")
    validate_invoice(invoice_id)
    send_to_tax(invoice_id)

@observe  # Inherits invoice_id from parent
def validate_invoice(invoice_id: str):
    observe.info("Validating invoice")
    # Validation logic

@observe
def send_to_tax(invoice_id: str):
    observe.info("Sending to tax system")
    # Tax logic
```

**Console Output:**
```
[11:30:00.100] ℹ️  process_invoice ▶️
  └─ invoice_id: INV-001
  [11:30:00.150] ℹ️  validate_invoice ▶️
    └─ invoice_id: INV-001
  [11:30:00.200] ✓ validate_invoice (50ms)
  [11:30:00.250] ℹ️  send_to_tax ▶️
    └─ invoice_id: INV-001
  [11:30:00.850] ✓ send_to_tax (600ms)
[11:30:00.900] ✓ process_invoice (800ms)
```

**Benefits:**
- ✅ No manual context threading
- ✅ Beautiful, nested output
- ✅ Automatic timing
- ✅ Stored in SQLite for querying
- ✅ Testable via trace mode

---

## ✅ Goals & Acceptance Criteria

### Primary Goals

1. **Context Propagation Without Manual Passing**
   - [ ] Tracked properties flow down call stack automatically
   - [ ] Child functions inherit parent's tracked properties
   - [ ] No need to pass tracked properties as parameters to `observe.info()`

2. **Additive Tracking**
   - [ ] Child can add NEW tracked properties to parent's context
   - [ ] Both parent and child properties visible in child's logs
   - [ ] Child's properties removed when child exits (scoped cleanup)

3. **Sticky Properties**
   - [ ] `sticky=True` flag keeps properties in context after function exits
   - [ ] Use case: `request_id`, `user_id`, `session_id` (cross-cutting concerns)
   - [ ] Sticky properties persist through entire call chain

4. **Override Behavior**
   - [ ] Child can override parent's tracked property value
   - [ ] Explicit `track` parameter takes precedence
   - [ ] Enables "switching context" mid-flow

5. **Trace Mode (Testing)**
   - [ ] `observe.trace_mode()` context manager captures events in-memory
   - [ ] Testing API: `trace.assert_operations()`, `trace.assert_tracked()`, etc.
   - [ ] Trace mode does NOT write to DB (memory-only)

6. **Beautiful Console Output**
   - [ ] ASCII tree rendering with indentation
   - [ ] Emojis for visual clarity (▶️, ✓, ❌, ℹ️)
   - [ ] Duration display (50ms, 1.2s, etc.)
   - [ ] Tracked properties displayed per operation

7. **SQLite Storage**
   - [ ] All events stored for historical analysis
   - [ ] Query API for filtering by operation, entity, date range
   - [ ] Efficient storage (WAL mode, append-only)

8. **Sync & Async Support**
   - [ ] One decorator works for both sync and async functions
   - [ ] Context propagation works in async/await

---

### Acceptance Criteria (Must Pass Before Merge)

- [ ] **AC1:** Context propagates without manual passing
  - Test: Parent tracks `invoice_id`, child logs without explicit parameter
  - Expected: Child's logs include `invoice_id`

- [ ] **AC2:** Additive tracking works
  - Test: Parent tracks `invoice_id`, child tracks `item_id`
  - Expected: Child's logs show both, parent's logs show only `invoice_id` after child exits

- [ ] **AC3:** Sticky properties persist
  - Test: Parent tracks `request_id` with `sticky=True`, multiple children execute
  - Expected: All children have `request_id` in logs, even after parent exits

- [ ] **AC4:** Override works
  - Test: Parent tracks `invoice_id="INV-001"`, child tracks `invoice_id="INV-002"`
  - Expected: Child's logs show `INV-002`

- [ ] **AC5:** Trace mode captures flow
  - Test: Execute function in trace mode, assert operations list
  - Expected: `trace.operations == ["parent", "child1", "child2"]`

- [ ] **AC6:** Console output is beautiful
  - Test: Visual inspection of TreeLogBackend output
  - Expected: Indented, emojis, durations, tracked properties

- [ ] **AC7:** SQLite storage works
  - Test: Execute function, query events from DB
  - Expected: Events retrievable with correct `parent_operation`, `data`, `duration_ms`

- [ ] **AC8:** Performance targets met
  - Test: Benchmark decorator overhead
  - Expected: <5% overhead vs no decorator

---

## 🏗️ Architecture

### File Structure

```
scriptman/observe/
├── __init__.py              # Public API (observe.info, observe.trace_mode, etc.)
├── models.py                # ObserverEvent dataclass
├── context.py               # Context propagation (contextvars)
├── decorator.py             # @observe implementation
├── storage.py               # SQLite storage client
├── backends/
│   ├── __init__.py
│   ├── base.py              # ObserverBackend ABC
│   ├── tree.py              # TreeLogBackend (console rendering)
│   └── sqlite.py            # SQLiteBackend (database storage)
├── trace.py                 # TraceMode class (testing utilities)
└── query.py                 # Query API (find, filter, aggregate)
```

### Core Components

I'll continue with the full detailed implementation in the next part, but this gives you the structure. The document is MASSIVE (over 15,000 words with all the code examples, testing requirements, etc.).

Should I continue pushing the complete document? It includes:
- Full code for all 8 core components
- Complete implementation plan (6 phases, 25+ tasks)
- All code examples (4 detailed scenarios)
- Complete testing requirements
- Decision log
- Progress tracker
- Performance targets
- Post-implementation review checklist

**Want me to push the FULL version now?** 🚀