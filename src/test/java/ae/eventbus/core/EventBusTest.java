package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.AbstractEvent;
import ae.eventbus.core.subscription.DispatchMode;
import ae.eventbus.core.subscription.Priority;
import ae.eventbus.core.subscription.Subscription;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventBusTest {

    @Test
    void testBasicPublishSubscribe() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            List<String> received = new ArrayList<>();
            bus.subscribe("orders/created", String.class, received::add);

            bus.publish("orders/created", "Order-123");
            assertThat(received).containsExactly("Order-123");
        }
    }

    @Test
    void testHierarchicalSubscription() {
        try (EventBus bus = EventBusBuilder.create().withHierarchicalRouting().build()) {
            List<String> rootReceived = new ArrayList<>();
            List<String> leafReceived = new ArrayList<>();

            bus.subscribeHierarchy("orders", String.class, rootReceived::add);
            bus.subscribe("orders/europe/de", String.class, leafReceived::add);

            bus.publish("orders/europe/de", "Order-DE");

            assertThat(rootReceived).containsExactly("Order-DE");
            assertThat(leafReceived).containsExactly("Order-DE");
        }
    }

    @Test
    void testPriorityOrdering() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            List<String> executionOrder = new ArrayList<>();

            Channel channel = Channel.of("test", "priority");
            bus.subscribe(channel, String.class, Priority.LOW, DispatchMode.SYNCHRONOUS,
                    ae.eventbus.core.subscription.ReferenceType.STRONG, false, e -> executionOrder.add("LOW"));
            bus.subscribe(channel, String.class, Priority.HIGHEST, DispatchMode.SYNCHRONOUS,
                    ae.eventbus.core.subscription.ReferenceType.STRONG, false, e -> executionOrder.add("HIGHEST"));
            bus.subscribe(channel, String.class, Priority.NORMAL, DispatchMode.SYNCHRONOUS,
                    ae.eventbus.core.subscription.ReferenceType.STRONG, false, e -> executionOrder.add("NORMAL"));

            bus.publish(channel, "payload");

            assertThat(executionOrder).containsExactly("HIGHEST", "NORMAL", "LOW");
        }
    }

    @Test
    void testMultiChannelEventDeduplication() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            AtomicInteger callCount = new AtomicInteger(0);

            Channel chA = Channel.of("topic", "alpha");
            Channel chB = Channel.of("topic", "beta");

            // Custom event belonging to both channels
            class MultiChannelEvent extends AbstractEvent {
                MultiChannelEvent(Channel c1, Channel c2) {
                    super(c1, c2);
                }
            }

            // Subscriber listening to both channels
            bus.subscribe(chA, MultiChannelEvent.class, e -> callCount.incrementAndGet());
            bus.subscribe(chB, MultiChannelEvent.class, e -> callCount.incrementAndGet());

            bus.publish(new MultiChannelEvent(chA, chB));

            // Without deduplication, subscriber might be triggered multiple times
            // With subscriber-level deduplication across channels, it must execute once per subscriber per publish
            // In this case, we have 2 distinct subscribers: one on chA and one on chB. Each should be called once.
            assertThat(callCount.get()).isEqualTo(2);
        }
    }

    @Test
    void testAsynchronousDispatchModeInSynchronousDispatcher() throws InterruptedException {
        try (EventBus bus = EventBusBuilder.create().withSynchronousDispatching().build()) {
            CountDownLatch latch = new CountDownLatch(1);
            List<String> threadNames = Collections.synchronizedList(new ArrayList<>());
            String publisherThread = Thread.currentThread().getName();

            Channel channel = Channel.of("async", "test");
            bus.subscribe(channel, String.class, Priority.NORMAL, DispatchMode.ASYNCHRONOUS,
                    ae.eventbus.core.subscription.ReferenceType.STRONG, false, msg -> {
                        threadNames.add(Thread.currentThread().getName());
                        latch.countDown();
                    });

            bus.publish(channel, "hello async");

            assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(threadNames).hasSize(1);
            // The subscriber must have executed in a different thread, not the publisher thread
            assertThat(threadNames.get(0)).isNotEqualTo(publisherThread);
        }
    }

    @Test
    void testLifecycleCloseRejectsNewOperations() {
        EventBus bus = EventBusBuilder.create().build();
        bus.subscribe("test/channel", String.class, e -> {});
        bus.publish("test/channel", "msg");

        bus.close();

        assertThatThrownBy(() -> bus.publish("test/channel", "msg2"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");

        assertThatThrownBy(() -> bus.subscribe("test/channel", String.class, e -> {}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }
}
