# Scriptman Vision Board

> **Mission:** Make Python automation observable, testable, and delightfully simple.

**Last Updated:** February 5, 2026  
**Version:** 3.0 Planning Phase

---

## 🎯 Core Philosophy

### "Simple Things Simple, Complex Things Possible"

Scriptman is designed with a **progressive disclosure** philosophy:
- **Beginners** get working code in 3 lines
- **Experts** get escape hatches and extensibility
- **Everyone** gets beautiful defaults and clear errors

### The Three Pillars

1. **Observability Without Invasion**
   - Tracking should be declarative, not manual
   - Context propagates automatically
   - Beautiful output by default

2. **SQLite-First Infrastructure**
   - Embedded, portable, zero-config
   - Powers cache, queue, observer storage
   - Extensible to other backends via ABCs

3. **One Decorator, Two Worlds**
   - Single `@observe` decorator for sync AND async
   - Unified API, different execution contexts
   - No `@observe_sync` vs `@observe_async` confusion

---

## 🏗️ Architecture Principles

### 1. Explicit Over Implicit
```python
# ✅ GOOD: Clear what's being tracked
@observe(track="invoice_id")
def process_invoice(invoice_id: str):
    pass

# ❌ BAD: Magic injection (confusing to readers)
@observe
def process_invoice():  # Where does invoice_id come from?!
    pass
```

**Rationale:** Code should be readable by someone unfamiliar with the framework.

---

### 2. Context Propagation Over Manual Passing
```python
# ✅ GOOD: Context flows automatically
@observe(track="invoice_id")
def parent(invoice_id: str):
    child()  # invoice_id in context

@observe  # Inherits invoice_id from parent
def child():
    observe.info("Processing")  # invoice_id auto-included in logs

# ❌ BAD: Manual threading of context
def parent(invoice_id: str):
    child(invoice_id)  # Tedious, error-prone

def child(invoice_id: str):
    observe.info("Processing", invoice_id=invoice_id)  # Repetitive
```

**Rationale:** Reduce boilerplate while maintaining function signature clarity.

---

### 3. Progressive Disclosure
```python
# Level 1: Beginner (just works)
@observe
def my_function():
    observe.info("Hello")

# Level 2: Intermediate (track specific properties)
@observe(track="user_id")
def process_user(user_id: str):
    observe.info("Processing user")

# Level 3: Advanced (custom backends, sticky properties)
@observe(
    track="request_id",
    sticky=True,
    backend=CustomBackend(),
    capture_args=True
)
def handle_request(request_id: str):
    observe.info("Handling request")
```

**Rationale:** Framework grows with user expertise.

---

### 4. Zero-Cost Abstractions
- If you don't use Observer → zero overhead
- If you use Observer → minimal overhead (<5%)
- Context propagation via `contextvars` (not thread locals)
- Lazy imports to avoid circular dependencies

**Implementation Detail:**
```python
# Only import observer when decorator is used
def observe(*args, **kwargs):
    from scriptman.observe import _observe
    return _observe(*args, **kwargs)
```

---

### 5. Testing Is a First-Class Use Case
```python
# Observer doubles as testing tool
def test_invoice_flow(trace):
    with trace:
        process_invoice("INV-001")
    
    trace.assert_operations(["validate", "post", "send_tax"])
    trace.assert_no_errors()
```

**Rationale:** Trace mode enables "observability-driven testing" paradigm.

---

## 🚀 Feature Domains

### 1. **Observer** (Observability & Tracing)
**Status:** v3.0 In Development  
**Purpose:** Make code execution visible without invasive logging

**Key Features:**
- Function-level observability via decorator
- Context propagation (tracked properties flow down stack)
- Sticky properties (cross-cutting concerns like `request_id`)
- Trace mode (testing + debugging)
- Beautiful console output (ASCII tree rendering)
- SQLite storage for historical analysis

**Design Values:**
- Declarative tracking (not manual logging)
- Additive context (child adds properties to parent's)
- Scoped cleanup (non-sticky properties removed on exit)
- Normal Python (no magic parameter injection)

---

### 2. **Database** (Multi-Backend Support)
**Status:** v2.x Stable  
**Purpose:** SQLite-first with extensibility to MSSQL, PostgreSQL, etc.

**Architecture:**
- `DatabaseClient` ABC (base interface)
- `SQLiteClient` (default, zero-config)
- Optional extras: `scriptman[mssql]`, `scriptman[postgres]`

**Design Values:**
- SQLite as default (embedded, portable)
- Connection pooling for production
- Lazy imports for heavy dependencies

---

### 3. **Cache** (Multi-Backend Caching)
**Status:** v2.x Stable  
**Purpose:** Fast key-value storage with TTL support

**Architecture:**
- `CacheBackend` ABC
- `SQLiteCache` (default)
- `RedisCache` (optional: `scriptman[redis]`)

**Design Values:**
- Consistent API across backends
- TTL and eviction policies
- Thread-safe by default

---

### 4. **Queue** (Event-Driven Workflows)
**Status:** v3.1 Planned  
**Purpose:** Kafka-inspired message queue using SQLite

**Design Values:**
- Consumer groups
- Offset tracking
- Retry logic
- Dead-letter queue

---

### 5. **CLI** (Introspection & Management)
**Status:** v3.1 Planned  
**Purpose:** Inspect observer data, manage cache, debug traces

**Commands:**
```bash
scriptman observe trace --function process_invoice
scriptman observe query --operation validate_invoice
scriptman cache clear --backend sqlite
```

---

## ❌ Anti-Patterns (What We Actively Avoid)

### 1. Magic Behavior
**Bad Example:**
```python
@observe(track="invoice_id")
def process(invoice_id: str):
    validate()  # ← invoice_id magically available as parameter

@observe
def validate():  # ← No invoice_id parameter, but it works?!
    pass
```

**Why Bad:** Confusing to readers, breaks type checkers, hides dependencies.

**Our Approach:** Parameters stay explicit, context is separate layer.

---

### 2. Vendor Lock-In
**Bad Example:**
```python
# Forces Redis usage
from scriptman import Cache
cache = Cache()  # Requires Redis to be installed
```

**Why Bad:** Limits portability, increases deployment complexity.

**Our Approach:** SQLite default, optional extras for other backends.

---

### 3. Framework Takeover
**Bad Example:**
```python
# Framework owns your main loop
scriptman.run(my_app)  # ← You lose control
```

**Why Bad:** Reduces flexibility, hard to integrate with existing code.

**Our Approach:** Decorators and utilities, not frameworks. You control execution.

---

### 4. Hidden Configuration
**Bad Example:**
```python
# Behavior changes based on environment variables
@observe  # ← Reads SCRIPTMAN_BACKEND from env, no indication in code
def my_function():
    pass
```

**Why Bad:** Non-deterministic, hard to debug, surprising behavior.

**Our Approach:** Explicit configuration, sane defaults, clear overrides.

---

## 🧪 Quality Standards

### Testing Philosophy

1. **Test Behavior, Not Implementation**
   - Use trace mode to verify flows
   - Assert on outcomes, not internal state
   - Example: `trace.assert_operations([...])` not `mock.assert_called()`

2. **Coverage Targets**
   - Core modules: 95%+
   - Utilities: 85%+
   - Integration tests for all public APIs

3. **Performance Benchmarks**
   - Context propagation: <1μs overhead
   - Event storage: <10ms per event
   - Trace mode: <5% overhead vs normal execution

---

### API Design Rules

1. **One Obvious Way**
   - Don't provide 5 ways to do the same thing
   - Example: `observe.info()` not `observe.log_info()` + `observe.log_message()`

2. **Consistent Naming**
   - `track` for property extraction (not `extract`, `capture`, `monitor`)
   - `sticky` for persistence (not `global`, `permanent`, `retain`)
   - `entity` for type hints (not `resource`, `object`, `kind`)

3. **Minimal Surprises**
   - Default behavior should be safe (e.g., trace mode doesn't write to DB)
   - Overrides should be explicit (e.g., `sticky=True` not auto-detected)

4. **Type Hints Everywhere**
   - All public APIs have full type annotations
   - Return types are concrete, not `Any`

---

### Documentation Standards

1. **Docstring Format**
   ```python
   def function(param: str) -> bool:
       """
       One-line summary.
       
       Longer explanation if needed. Show use cases.
       
       Args:
           param: Description with examples
       
       Returns:
           Description of return value
       
       Example:
           >>> function("test")
           True
       """
   ```

2. **README-Driven Development**
   - Write docs BEFORE implementation
   - Docs describe the API we WANT, not what exists
   - Implementation follows docs

3. **Real Examples**
   - Every feature has runnable example
   - Examples use realistic data (not `foo`, `bar`, `baz`)

---

## 🗓️ Roadmap

### v3.0 (Current): Observer Foundation
**Timeline:** Feb-Mar 2026  
**Scope:**
- ✅ ObserverEvent model
- ✅ Context propagation (with sticky flag)
- ✅ @observe decorator (sync, async-ready)
- ✅ SQLite storage backend
- ✅ Query API
- ✅ TreeLogBackend (console output)
- ✅ Manual logging (observe.info/debug/error)
- ✅ Trace mode (with testing API)

**Success Criteria:**
- Full test coverage (95%+)
- Documentation complete
- Real-world usage in 3+ projects

---

### v3.1 (Next): CLI & Web UI
**Timeline:** Apr-May 2026  
**Scope:**
- CLI commands (trace, query, analyze)
- Web dashboard (view traces, search operations)
- Static analysis mode (visualize flow without execution)
- Performance optimizations (connection pooling, batch writes)

**Success Criteria:**
- CLI dogfooded in daily development
- Web UI usable for debugging production issues

---

### v4.0 (Future): Distributed Tracing
**Timeline:** Q3 2026  
**Scope:**
- Distributed trace IDs (correlate across services)
- OpenTelemetry compatibility
- Remote storage backends (PostgreSQL, Elasticsearch)
- Sampling strategies (trace 1% of requests)

**Success Criteria:**
- Used in microservices architecture
- Compatible with industry standards (OTEL)

---

## 🎨 Design Values (Priority Order)

1. **Developer Experience** (DX)
   - Beautiful errors, not cryptic tracebacks
   - Tab-completion works everywhere
   - Fast feedback loops

2. **Simplicity**
   - Fewer features, better executed
   - Delete code aggressively
   - Resist feature creep

3. **Performance**
   - Fast enough for real-time logging
   - Efficient storage (SQLite's append-only WAL)
   - Lazy evaluation where possible

4. **Extensibility**
   - ABCs for custom backends
   - Plugin system (future)
   - Don't paint ourselves into corners

5. **Portability**
   - Works on Windows, macOS, Linux
   - No native dependencies (pure Python when possible)
   - Graceful degradation (fallback to print if SQLite fails)

---

## 🔄 Decision Framework

When adding a feature, ask:

1. **Is it aligned with the mission?**
   - Does it make automation more observable/testable/simple?

2. **Does it follow the principles?**
   - Explicit? Progressive? Zero-cost?

3. **What's the simplest version?**
   - Can we ship 80% of the value with 20% of the complexity?

4. **What's the escape hatch?**
   - Can power users override/extend if needed?

5. **How will we test it?**
   - Is trace mode sufficient, or do we need new testing primitives?

---

## 📚 Inspiration (Libraries We Admire)

### requests
- **What we learned:** Simple API, complex internals
- **Applied to Scriptman:** `@observe` is simple, context propagation is complex

### click
- **What we learned:** Progressive disclosure (simple commands → groups → plugins)
- **Applied to Scriptman:** `observe.info()` → `@observe(track=...)` → custom backends

### FastAPI
- **What we learned:** Type hints enable great DX
- **Applied to Scriptman:** Full type annotations, pydantic models

### SQLAlchemy
- **What we learned:** ABCs enable extensibility without coupling
- **Applied to Scriptman:** `DatabaseClient`, `CacheBackend` ABCs

### pytest
- **What we learned:** Testing can be delightful
- **Applied to Scriptman:** Trace mode as testing primitive

---

## 🧭 North Star Metrics

How do we know we're succeeding?

1. **Adoption:** 3+ production projects using Observer by v3.0 launch
2. **DX:** "This is amazing!" reactions in first 5 minutes
3. **Performance:** <5% overhead in real-world usage
4. **Test Coverage:** 95%+ on core modules
5. **Documentation:** Every feature has runnable example

---

## 🔮 Future Vision (5 Years)

**Scriptman becomes the Go-To for:**
- Python automation observability
- Testing automation workflows
- Debugging production automation scripts

**Success looks like:**
- "Just add `@observe` and you'll see what's happening"
- Community-contributed backends (Datadog, New Relic)
- Case studies from major companies

---

## 📝 Living Document

This vision board evolves as we learn. When to update:

- ✅ New principle discovered during implementation
- ✅ Architecture decision has broader implications
- ✅ User feedback reveals better approach
- ❌ Don't update for minor implementation details (use Implementation Doc)

**Review Cadence:** After each major feature (v3.0, v3.1, v4.0)

---

**Remember:** We're building a library, not a framework. Libraries empower, frameworks constrain. Stay true to that.

---

*"Simplicity is prerequisite for reliability." - Edsger Dijkstra*