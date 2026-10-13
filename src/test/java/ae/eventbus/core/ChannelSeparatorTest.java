package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.subscription.Subscription;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that all string-path methods honour the bus's configured channel separator,
 * not the hardcoded default {@code "/"}.
 */
class ChannelSeparatorTest {

    @Test
    void testSubscribeAndPublishWithDotSeparator() {
        try (EventBus bus = EventBusBuilder.create().withChannelSeparator(".").build()) {
            List<String> received = new ArrayList<>();
            bus.subscribe("orders.europe.de", String.class, received::add);

            bus.publish("orders.europe.de", "Order-DE");

            assertThat(received).containsExactly("Order-DE");
        }
    }

    @Test
    void testSubscribeHierarchyWithDotSeparator() {
        try (EventBus bus = EventBusBuilder.create()
                .withChannelSeparator(".")
                .withHierarchicalRouting()
                .build()) {

            List<String> rootReceived = new ArrayList<>();
            List<String> leafReceived = new ArrayList<>();

            bus.subscribeHierarchy("orders", String.class, rootReceived::add);
            bus.subscribe("orders.europe.de", String.class, leafReceived::add);

            bus.publish("orders.europe.de", "Order-DE");

            assertThat(rootReceived).containsExactly("Order-DE");
            assertThat(leafReceived).containsExactly("Order-DE");
        }
    }

    @Test
    void testHandleRequestWithDotSeparator() throws Exception {
        try (EventBus bus = EventBusBuilder.create().withChannelSeparator(".").build()) {
            bus.handleRequest("rpc.ping", String.class, req -> "PONG:" + req);

            CompletableFuture<String> future = bus.request("rpc.ping", "hello", String.class, Duration.ofSeconds(2));
            String result = future.get(2, TimeUnit.SECONDS);

            assertThat(result).isEqualTo("PONG:hello");
        }
    }

    @Test
    void testSubscribeWeakWithDotSeparator() {
        try (EventBus bus = EventBusBuilder.create().withChannelSeparator(".").build()) {
            AtomicInteger count = new AtomicInteger(0);
            Object target = new Object();

            bus.subscribeWeak("metrics.cpu", Object.class, target, (t, e) -> count.incrementAndGet());

            bus.publish("metrics.cpu", new Object());

            assertThat(count.get()).isEqualTo(1);
        }
    }

    @Test
    void testDefaultSlashSeparatorRegressionGuard() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            List<String> received = new ArrayList<>();
            bus.subscribe("a/b/c", String.class, received::add);

            bus.publish("a/b/c", "value");

            assertThat(received).containsExactly("value");
        }
    }

    @Test
    void testGetSubscribersWithDotSeparator() {
        try (EventBus bus = EventBusBuilder.create().withChannelSeparator(".").build()) {
            Subscription sub = bus.subscribe("app.events", String.class, e -> {});

            assertThat(bus.getSubscribers("app.events")).hasSize(1);
            sub.unsubscribe();
        }
    }

    @Test
    void testGetHistoryWithDotSeparator() {
        try (EventBus bus = EventBusBuilder.create().withChannelSeparator(".").build()) {
            bus.subscribe("app.events", String.class, e -> {});
            bus.publish("app.events", "hello");

            assertThat(bus.getHistory("app.events")).hasSize(1);
        }
    }
}
