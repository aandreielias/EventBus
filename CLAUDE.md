# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Build
mvn clean package

# Run all tests
mvn test

# Run a single test class
mvn test -Dtest=EventBusTest

# Run a single test method
mvn test -Dtest=EventBusTest#testBasicPublishSubscribe

# Install to local Maven repo
mvn clean install
```

Java 21 is required (project uses virtual threads and source/target set to 21).

## Architecture

This is a zero-runtime-dependency publish-subscribe event bus library (`ae.eventbus.core.*`). All core classes live under `src/main/java/ae/eventbus/core/`.

### Entry points

`EventBus` (interface) is the public facade. `DefaultEventBus` is its orchestrator — it delegates to a router, dispatcher, registry, and validator. Instances are created via the fluent `EventBusBuilder`.

```java
EventBus bus = EventBusBuilder.create()
    .withHierarchicalRouting()
    .withAsynchronousDispatching()
    .build();
```

### Channel addressing

`Channel` is an immutable value object that models hierarchical paths (`"ui/button/click"`). It stores segments internally (decoupled from the delimiter) and supports parent navigation via `getParent()`, `isParentOf()`, `isChildOf()`. Factory methods: `Channel.of(...)` and `Channel.parse(...)`.

### Strategy pattern for pluggability

Four pluggable strategies are wired in by the builder:

| Strategy | Interface | Default |
|---|---|---|
| Routing | `EventRouter` | `HierarchicalRouter` |
| Dispatching | `EventDispatcher` | `SynchronousDispatcher` |
| Validation | `EventValidator` | `DefaultEventValidator` |
| Exception handling | `SubscriberExceptionHandler` | `LoggingExceptionHandler` |

`HierarchicalRouter` walks the parent chain of a `Channel` to collect matching subscribers (parent subscribers receive child-channel events). `ExactMatchRouter` is an O(1) alternative with no inheritance.

`SynchronousDispatcher` runs synchronous subscribers on the caller thread and offloads `ASYNCHRONOUS` subscribers to a virtual-thread `ExecutorService`. `AsynchronousDispatcher` puts all subscribers on the thread pool.

### Subscription model

`subscribe()` returns a `Subscription` (`AutoCloseable`). Each subscription has a `Priority` (HIGHEST → MONITOR), `DispatchMode` (SYNCHRONOUS / ASYNCHRONOUS), and `ReferenceType` (STRONG / WEAK). `WEAK` subscriptions are GC-eligible — useful for listener objects with managed lifecycles.

Payloads are wrapped transparently via `EventEnvelope<T>` when a raw value (not an `Event`) is published.

### Request-response

`bus.request(channel, payload)` returns a `CompletableFuture`. Handlers registered with `registerRequestHandler()` respond via `RequestEnvelope`. `NoResponderException` is thrown on timeout.

### Registry & introspection

`EventRegistry` records publication history (ring buffer, default 10 000 events) and tracks live channel/subscriber metadata. Access via `bus.getChannels()`, `bus.getSubscribers()`, `bus.getHistory()`. Use `NoOpEventRegistry` to disable overhead in high-throughput scenarios.

### Thread safety

- `ConcurrentHashMap` + `CopyOnWriteArrayList` for lock-free subscriber iteration
- `AtomicBoolean` for idempotent `unsubscribe()`
- No global locks on hot paths; virtual threads for async dispatch
