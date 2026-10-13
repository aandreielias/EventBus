# EventBus Core

A pub/sub event bus for Java 21. No runtime dependencies. Drop it in, wire up your handlers, done.

The core idea: components publish events to named channels, other components subscribe to those channels. They never need to know about each other. You can route events hierarchically, set handler priorities, go async, do request/response, and configure almost everything through the builder.

**Requires:** Java 21+

---

## Build

```bash
mvn clean package
```

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

Available levels: `HIGHEST(100)`, `HIGH(75)`, `NORMAL(50)`, `LOW(25)`, `MONITOR(0)`.

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

---

## Subscriptions

`subscribe()` returns a `Subscription` you can cancel at any time.

```java
Subscription sub = bus.subscribe("chat/messages", Message.class, msg -> display(msg));

// later...
sub.unsubscribe();
```

`Subscription` is also `AutoCloseable`, so try-with-resources works.

---

## Weak references

Use `subscribeWeak` when you don't want the bus to keep your object alive. Once the target is garbage collected, the subscription silently deregisters itself.

```java
bus.subscribeWeak("ui/events", UIEvent.class, this, MyController::onEvent);
```

Good for UI components or anything with a lifecycle managed elsewhere.

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

Requests time out after 5 seconds by default. Override per-call or globally:

```java
bus.request(channel, req, Response.class, Duration.ofSeconds(10));

EventBusBuilder.create().withDefaultRequestTimeout(Duration.ofSeconds(30)).build();
```

---

## Dead-letter handler

Called whenever an event is published to a channel with no subscribers.

```java
EventBus bus = EventBusBuilder.create()
    .withDeadLetterHandler(event -> log.warn("Undelivered: {}", event))
    .build();
```

Exceptions thrown by the dead-letter handler are swallowed — they never reach the publisher.

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

All string-path methods (subscribe, publish, request, etc.) use the separator you configured.

---

## Builder options

```java
EventBusBuilder.create()
    .withHierarchicalRouting()           // or .withExactRouting() for strict matching
    .withAsynchronousDispatching()       // or supply your own ExecutorService
    .withChannelSeparator(".")
    .withDefaultReferenceType(ReferenceType.STRONG)
    .withDefaultRequestTimeout(Duration.ofSeconds(10))
    .withMaxHistorySize(500)             // ring-buffer size for event history
    .withMaxChannels(5_000)             // cap on tracked channels in the registry
    .withDeadLetterHandler(e -> ...)
    .withNoOpRegistry()                  // disables history/introspection for max throughput
    .withNoOpValidator()                 // disables channel name validation
    .withExceptionHandler(myHandler)     // custom subscriber exception strategy
    .preRegisterChannels(ch1, ch2)       // pre-warm channels before first publish
    .build();
```

---

## Introspection

```java
bus.getChannels();                         // all channels with active subscribers
bus.getSubscribers();                      // all active subscribers
bus.getSubscribers("orders/created");      // subscribers on a specific channel
bus.getHistory();                          // recent publications (ring buffer)
bus.getHistory("orders/created");          // filtered by channel

bus.getRegistry().getChannelInfo(channel)
    .ifPresent(info -> {
        System.out.println(info.getPublicationCount());
        System.out.println(info.toSnapshot()); // immutable point-in-time copy
    });
```

Introspection is a live view into the bus — useful for dashboards, debugging, and audit logging. Use `withNoOpRegistry()` to disable it completely if you don't need it.

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
