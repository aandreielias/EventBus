# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Build (jar is also the CLI: Main-Class = ae.eventbus.cli.EventBusCli)
mvn clean package

# Run all tests
mvn test

# Run a single test class
mvn test -Dtest=EventBusTest

# Run a single test method
mvn test -Dtest=EventBusTest#testBasicPublishSubscribe

# Install to local Maven repo
mvn clean install

# Run the CLI against a local JVM that registered a bus with EventBusAdmin
java -jar target/eventbus-core-1.0.0-SNAPSHOT.jar --pid <pid> channels
```

Java 21 is required (project uses virtual threads and `release` is set to 21). The CLI's `--pid` attach needs a full JDK (`jdk.attach` module).

## Architecture

This is a zero-runtime-dependency publish-subscribe event bus library. The library lives under `src/main/java/ae/eventbus/core/`; the terminal client lives under `src/main/java/ae/eventbus/cli/`. Both only use the JDK (JMX and the attach API are part of it).

### Entry points

`EventBus` (interface) is the public facade. `DefaultEventBus` is its orchestrator — it delegates to a router, dispatcher, registry, and validator. Instances are created via the fluent `EventBusBuilder`. A builder can be reused; each `build()` creates fresh router, dispatcher and registry instances.

```java
EventBus bus = EventBusBuilder.create()
    .withHierarchicalRouting()
    .withAsynchronousDispatching()
    .build();
```

### Channel addressing

`Channel` is an immutable value object that models hierarchical paths (`"ui/button/click"`). It stores segments internally (decoupled from the delimiter) and supports parent navigation via `getParent()`, `isParentOf()`, `isChildOf()`. Factory methods: `Channel.of(...)` and `Channel.parse(...)`. `DefaultEventValidator` limits depth to 16 and segments to `[a-zA-Z0-9_.-]`, and rejects segments containing the bus's separator (the builder passes it in), so every channel round-trips through its string path.

### Strategy pattern for pluggability

Four pluggable strategies are wired in by the builder:

| Strategy | Interface | Default |
|---|---|---|
| Routing | `EventRouter` | `HierarchicalRouter` |
| Dispatching | `EventDispatcher` | `SynchronousDispatcher` |
| Validation | `EventValidator` | `DefaultEventValidator` |
| Exception handling | `SubscriberExceptionHandler` | `LoggingExceptionHandler` |

`HierarchicalRouter` walks the parent chain of a `Channel` to collect matching subscribers (parent subscribers with `handlesChannelChildren` receive child-channel events). `ExactMatchRouter` is an O(1) alternative with no inheritance. Both share `ChannelSubscriberMap`, which registers/removes inside `ConcurrentHashMap.compute` so concurrent subscribe/unsubscribe cannot drop a subscriber.

`SynchronousDispatcher` runs synchronous subscribers on the caller thread and offloads `ASYNCHRONOUS` subscribers to a virtual-thread `ExecutorService`. `AsynchronousDispatcher` puts all subscribers on the executor. Both delegate to the package-private `SubscriberInvoker`: async work is capped (`withMaxConcurrentAsyncTasks`, default 10 000) and runs on the publisher thread when the cap is hit (caller-runs backpressure); a throwing `SubscriberExceptionHandler` is logged and never stops delivery.

### Subscription model

`subscribe()` returns a `Subscription` (`AutoCloseable`). Each subscription has a `Priority` (HIGHEST → MONITOR), `DispatchMode` (SYNCHRONOUS / ASYNCHRONOUS), and `ReferenceType` (STRONG / WEAK). Handlers are always held strongly. `WEAK` ties the subscription's lifetime to a separate *subscriber target* object (use `subscribeWeak(...)`); `WEAK` without a target throws `IllegalArgumentException`. `DefaultEventBus` indexes live subscriptions by id (`getSubscription(UUID)`, `unsubscribe(UUID)`) and removes them from the index when they end. `Subscriber` counts delivered and failed invocations. Subscribers sort by priority, then creation order (a global counter), so equal priorities run first-come first-served.

Payloads are wrapped transparently via `EventEnvelope<T>` when a raw value (not an `Event`) is published. `EventEnvelope.toString()` deliberately omits the payload.

Events with no matching subscriber go to the optional dead-letter handler (`withDeadLetterHandler`); its exceptions are logged, never propagated.

### Request-response

`bus.request(channel, request, ResponseType.class[, timeout])` returns a `CompletableFuture`; `requestAll` gathers every handler's response and `requestFrom` targets one handler by subscription id. Handlers are registered with `handleRequest(channel, RequestType.class, handler)` and simply return the response; `RequestEnvelope` carries it internally. Request handlers are always `ASYNCHRONOUS`, so the timeout applies even with `SynchronousDispatcher`. Plain `subscribe` with a `RequestEnvelope` type is rejected, so only request handlers see requests. `request` completes with the first successful response and `requestAll` with all successful ones; failed handlers are skipped, and the future fails with the first failure only if every handler failed. It also fails with `NoResponderException` when no handler accepts the request type, `TimeoutException` after the timeout (default 5 s), `ClassCastException` when the response is not a `ResponseType`, and `IllegalStateException` when the bus is closed first.

### Registry & introspection

`EventRegistry` records publication history (bounded buffer, default 1 000 records) and channel statistics (default cap 10 000 channels), and exposes live subscriber metadata. Every `EventRecord` has a monotonically increasing `sequence` (survives `clearHistory()`), used as a polling cursor. Access via `bus.getChannels()`, `bus.getSubscribers()`, `bus.getHistory()`. Use `withNoOpRegistry()` (`NoOpEventRegistry`) to disable overhead in high-throughput scenarios — this also empties all admin/CLI views.

### Admin (JMX) and CLI

- `ae.eventbus.core.admin.EventBusAdmin` registers a bus as the MXBean `ae.eventbus:type=EventBus,name=<name>` (`EventBusAdminMXBean`, records of strings/numbers only). `AdminAccess.READ_ONLY` rejects `unsubscribe`/`clearHistory` with `SecurityException`; `READ_WRITE` allows them. This is not a security boundary (JMX access implies access to every platform MBean). Closing the bus does not unregister the bean; registering a new bus under a closed bus's name replaces it.
- `ae.eventbus.cli.EventBusCli` connects via `--pid` (attach API, local agent) or `--host`/`--url` (remote JMX, `--user` + `EBUS_PASSWORD`/prompt) and runs one command or an interactive shell. `Commands` holds the command table; `AdminClient` owns the connection and bus selection; `TextTable` renders output. Exit codes: 0 ok, 1 failure, 2 usage/target error.

### Thread safety

- `ConcurrentHashMap` + `CopyOnWriteArrayList` for lock-free subscriber iteration; mutations of a channel's list go through `compute`
- `AtomicBoolean` for idempotent `unsubscribe()` and `close()`
- No global locks on hot paths; virtual threads for async dispatch
- `close()` fails pending requests, ends all subscriptions, then shuts down the dispatcher (waits up to 5 s)
