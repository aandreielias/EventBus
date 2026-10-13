# EventBus Core

A pub/sub event bus for Java 21. No runtime dependencies. Drop it in, wire up your handlers, done.

The core idea: components publish events to named channels, other components subscribe to those channels. They never need to know about each other. You can route events hierarchically, set handler priorities, go async, do request/response, and configure almost everything through the builder.

**Requires:** Java 21+

---

## Build

```bash
mvn clean package
```

The resulting `target/eventbus-core-<version>.jar` is both the library and the [terminal client](#terminal-cli).

---

## Basics

```java
try (EventBus bus = EventBusBuilder.create().build()) {

    bus.subscribe("orders/created", String.class, orderId -> {
        System.out.println("New order: " + orderId);
    });

    bus.publish("orders/created", "ORD-1042");
}
```

Channels are path strings. The bus is `AutoCloseable` — always use try-with-resources or call `close()` explicitly.

---

## Hierarchical routing

Subscribe to a parent channel and receive events from all its children.

```java
EventBus bus = EventBusBuilder.create()
    .withHierarchicalRouting()
    .build();

// Receives events from "orders/eu/de", "orders/eu/fr", "orders/us/ny", etc.
bus.subscribeHierarchy("orders", Order.class, order -> audit(order));

// Only receives events from "orders/eu/de"
bus.subscribe("orders/eu/de", Order.class, order -> processGermany(order));

bus.publish("orders/eu/de", new Order(...));
// → both handlers fire
```

---

## Priority

When multiple subscribers are on the same channel, they fire in priority order.

```java
bus.subscribe(channel, String.class, Priority.HIGHEST, DispatchMode.SYNCHRONOUS,
        ReferenceType.STRONG, false, msg -> validate(msg));

bus.subscribe(channel, String.class, Priority.NORMAL, DispatchMode.SYNCHRONOUS,
        ReferenceType.STRONG, false, msg -> process(msg));

bus.subscribe(channel, String.class, Priority.MONITOR, DispatchMode.SYNCHRONOUS,
        ReferenceType.STRONG, false, msg -> log(msg));
```

Available levels: `HIGHEST(100)`, `HIGH(75)`, `NORMAL(50)`, `LOW(25)`, `MONITOR(0)`. Subscribers with the same priority run in the order they subscribed. Priority orders synchronous subscribers; async subscribers start in that order but run concurrently.

---

## Async dispatch

Mark individual subscribers as async — they run on a virtual thread, the publisher doesn't wait.

```java
bus.subscribe(channel, Report.class, Priority.NORMAL, DispatchMode.ASYNCHRONOUS,
        ReferenceType.STRONG, false, report -> sendEmail(report));
```

Or make all subscribers async by default:

```java
EventBus bus = EventBusBuilder.create()
    .withAsynchronousDispatching()
    .build();
```

Async work is bounded: at most 10 000 async invocations run at once (change with `withMaxConcurrentAsyncTasks(n)`). Beyond that, invocations run on the publishing thread, which slows publishers down instead of piling up unbounded work.

Async handlers run on other threads, so thread-locals (security context, MDC, `@Transactional`) do not carry over. Pass what the handler needs inside the event.

---

## Subscriptions

`subscribe()` returns a `Subscription` you can cancel at any time.

```java
Subscription sub = bus.subscribe("chat/messages", Message.class, msg -> display(msg));

// later...
sub.unsubscribe();
```

`Subscription` is also `AutoCloseable`, so try-with-resources works.

You can also look up or end a subscription by its id, for example from an admin tool:

```java
bus.getSubscription(id);   // Optional<Subscription>
bus.unsubscribe(id);       // true if it was live
```

Ended subscriptions are removed from the bus right away, so subscribe/unsubscribe churn does not use up memory.

---

## Weak references

Use `subscribeWeak` when you don't want the bus to keep your object alive. Once the target is garbage collected, the subscription silently deregisters itself.

```java
bus.subscribeWeak("ui/events", UIEvent.class, this, MyController::onEvent);
```

Good for UI components or anything with a lifecycle managed elsewhere.

The handler is always held strongly. Only the *target* is weak. `ReferenceType.WEAK` without a target throws `IllegalArgumentException`, because a weakly held lambda would be garbage collected almost immediately and silently stop receiving events.

---

## Request / response

One publisher, one or more handlers, each returns a value.

```java
// Register a handler
bus.handleRequest("pricing/quote", QuoteRequest.class, req -> {
    return new Quote(req.itemId(), calculatePrice(req));
});

// Send a request, get a future back
CompletableFuture<Quote> future = bus.request("pricing/quote", new QuoteRequest("SKU-99"), Quote.class);
Quote quote = future.get(3, TimeUnit.SECONDS);
```

Collect responses from all handlers:

```java
CompletableFuture<List<Quote>> all = bus.requestAll("pricing/quote", new QuoteRequest("SKU-99"), Quote.class);
```

Target a specific handler by its subscription ID:

```java
Subscription handler = bus.handleRequest("compute/job", Job.class, job -> process(job));
bus.requestFrom(handler.getId(), "compute/job", new Job(...), Result.class);
```

Request handlers always run on the async executor, so `request()` returns immediately and the timeout always applies. As with async subscribers, thread-locals of the requesting thread are not visible inside the handler.

`request()` completes with the first *successful* response. A failing handler doesn't fail the request while another handler can still answer. `requestAll()` returns the successful responses and leaves failed handlers out. Both fail only when every handler fails.

Only `handleRequest` handlers can see requests. Subscribing to `RequestEnvelope` directly throws `IllegalArgumentException`, so no other subscriber can read or answer someone else's request.

Requests time out after 5 seconds by default. Override per-call or globally:

```java
bus.request(channel, req, Response.class, Duration.ofSeconds(10));

EventBusBuilder.create().withDefaultRequestTimeout(Duration.ofSeconds(30)).build();
```

The future fails with:

| Exception | When |
|---|---|
| `NoResponderException` | no handler accepts the request type on that channel |
| *(the handler's exception)* | every matching handler failed (the first failure is the cause) |
| `TimeoutException` | no response within the timeout |
| `ClassCastException` | the handler returned something that is not the requested response type |
| `IllegalStateException` | the bus was closed while the request was pending |

---

## Dead-letter handler

Called whenever an event is published and no subscriber matches it.

```java
EventBus bus = EventBusBuilder.create()
    .withDeadLetterHandler(event -> log.warn("Undelivered: {}", event))
    .build();
```

Exceptions thrown by the dead-letter handler are logged and never reach the publisher.

---

## Custom channel separator

The default separator is `/`. Change it if your channel naming uses something else.

```java
EventBus bus = EventBusBuilder.create()
    .withChannelSeparator(".")
    .build();

bus.subscribe("app.services.auth", AuthEvent.class, e -> handle(e));
bus.publish("app.services.auth", new AuthEvent(...));
```

All string-path methods (subscribe, publish, request, etc.) use the separator you configured. Channel segments may not contain the separator, so `Channel.of("app.services")` is rejected on a `.`-separated bus. Write `Channel.of("app", "services")` or `"app.services"` instead.

---

## Builder options

```java
EventBusBuilder.create()
    .withHierarchicalRouting()           // or .withExactRouting() for strict matching
    .withAsynchronousDispatching()       // or supply your own ExecutorService (the bus shuts it down on close)
    .withMaxConcurrentAsyncTasks(1_000)  // async cap; beyond it work runs on the publisher (default 10 000)
    .withChannelSeparator(".")
    .withDefaultRequestTimeout(Duration.ofSeconds(10))
    .withMaxHistorySize(500)             // history buffer size (default 1 000)
    .withMaxChannels(5_000)              // cap on tracked channels in the registry (default 10 000)
    .withDeadLetterHandler(e -> ...)
    .withNoOpRegistry()                  // disables history/introspection for max throughput
    .withNoOpValidator()                 // disables channel name validation
    .withExceptionHandler(myHandler)     // custom subscriber exception strategy
    .preRegisterChannels(ch1, ch2)       // pre-warm channels before first publish
    .build();
```

A builder can be reused. Each `build()` creates an independent bus.

---

## Introspection

```java
bus.getChannels();                         // all channels with active subscribers
bus.getSubscribers();                      // all active subscribers
bus.getSubscribers("orders/created");      // subscribers on a specific channel
bus.getHistory();                          // recent publications, oldest first
bus.getHistory("orders/created");          // filtered by channel

bus.getRegistry().getChannelInfo(channel)
    .ifPresent(info -> {
        System.out.println(info.getPublicationCount());
        System.out.println(info.toSnapshot()); // immutable point-in-time copy
    });
```

Introspection is a live view into the bus. It is useful for dashboards, debugging, and audit logging. Use `withNoOpRegistry()` to disable it completely if you don't need it.

- Each `SubscriberInfo` carries `deliveredCount()` and `failedCount()`.
- Each `EventRecord` has a `getSequence()` number that keeps increasing across `clearHistory()`, so you can use it as a cursor to fetch only newer records.
- History stores metadata only. Payloads are never retained, and `EventEnvelope.toString()` omits the payload, so logging an envelope doesn't leak data.

---

## Publishing your own event objects

Instead of plain payloads, publish full event objects to multiple channels at once.

```java
public class OrderShipped extends AbstractEvent {
    public OrderShipped(Channel primary, Channel audit) {
        super(primary, audit);
    }
}

bus.publish(new OrderShipped(
    Channel.of("orders", "shipped"),
    Channel.of("audit", "log")
));
```

Each subscriber is invoked at most once per publication even if the event spans multiple channels.

---

## Terminal CLI

You can view and control a running application's bus from a terminal. The bus is exposed over JMX, which is built into the JDK, so this adds no dependencies.

### 1. Expose the bus in your application

```java
EventBus bus = EventBusBuilder.create().build();
EventBusAdmin admin = EventBusAdmin.register(bus, "club-server", AdminAccess.READ_ONLY);

// on shutdown
admin.close();   // unregisters the MBean
bus.close();
```

| Access | Allows |
|---|---|
| `READ_ONLY` | `info`, `channels`, `subscribers`, `history`, `tail` |
| `READ_WRITE` | all of the above, plus `unsubscribe` and `clear-history` |

The bean is registered as `ae.eventbus:type=EventBus,name=club-server`, so `jconsole` and VisualVM can read it too. Admin views rely on the registry: a bus built with `withNoOpRegistry()` shows nothing.

Closing the bus does not unregister the bean. Its `info` then shows `State: CLOSED`. Call `admin.close()` to remove it. Registering a new bus under the name of a closed one replaces the old bean automatically.

### 2. Connect

```bash
alias ebus='java -jar /path/to/eventbus-core-2.0.0-SNAPSHOT.jar'

ebus --pid 4242 channels             # local JVM, attach by process id
ebus --pid 4242                      # interactive shell
ebus --host app01:9010 --user ops info   # remote JMX agent
```

| Option | Meaning |
|---|---|
| `--pid <pid>` | Attach to a local JVM. It must run as the same OS user. No network port is opened. |
| `--host <host:port>` | Connect to a remote JMX agent. |
| `--url <jmx-url>` | Connect with a full JMX service URL. |
| `--bus <name>` | Choose a bus when the JVM registered more than one. |
| `--user <name>` | JMX user. The password is read from `EBUS_PASSWORD` or prompted. It is never taken as a flag. |

`--pid` needs a full JDK (the `jdk.attach` module), not a JRE.

### Commands

| Command | Description |
|---|---|
| `info` | Bus summary and configuration |
| `buses` | Buses registered in the JVM |
| `use <bus>` | Select the bus for later commands |
| `channels` | Channels with subscriber and publication counts |
| `subscribers [channel]` | Live subscribers with priority, mode, delivered/failed counts |
| `history [-n <count>] [channel]` | Recent publications. A channel includes its descendants. |
| `tail [--interval <ms>] [channel]` | Follow publications live. Press Enter to stop. Reports when events were skipped (only when following all channels). |
| `unsubscribe <subscription-id>` | End a subscription (needs `READ_WRITE`) |
| `clear-history` | Clear the publication history (needs `READ_WRITE`) |
| `help` | List commands |

In the shell, `exit` or `quit` leaves.

Exit codes:
- `0`: success
- `1`: connection, permission or remote failure
- `2`: usage error or invalid target

```text
$ ebus --pid 4242 channels
CHANNEL           SUBSCRIBERS  PUBLICATIONS  LAST FIRED
bookings          1            0             -
bookings/created  1            14            2026-10-13 22:50:37
members/joined    0            7             2026-10-13 22:50:36
members/lookup    1            0             -
```

### Security

- `AdminAccess` only limits this bean. It is **not** a security boundary. Anyone who can connect over JMX can also use the JVM's built-in management beans, such as heap dumps (which contain your data) and thread dumps. Treat JMX access as full access to the JVM.
- Use `READ_ONLY` in production unless operators really need control commands.
- Prefer `--pid`. It works only for the same OS user on the same machine and opens no port.
- `--pid` starts the JVM's local JMX agent, which keeps running after the CLI exits. It accepts only the same OS user.
- If you enable remote JMX (`-Dcom.sun.management.jmxremote.port=...`), always turn on authentication, an access file and TLS: `com.sun.management.jmxremote.authenticate=true`, `com.sun.management.jmxremote.access.file=...` and `com.sun.management.jmxremote.ssl=true`. Never expose the port to the internet.
- The CLI cannot publish events. That is deliberate: nothing typed in a terminal can bypass your application's own validation.
