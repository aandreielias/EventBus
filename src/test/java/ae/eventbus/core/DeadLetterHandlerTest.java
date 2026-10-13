package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Verifies the dead-letter handler — invoked when an event has no active subscribers.
 */
class DeadLetterHandlerTest {

    @Test
    void testDeadLetterHandlerInvokedWhenNoSubscribers() {
        AtomicInteger deadLetterCount = new AtomicInteger(0);

        try (EventBus bus = EventBusBuilder.create()
                .withDeadLetterHandler(event -> deadLetterCount.incrementAndGet())
                .build()) {

            bus.publish("orphan/channel", "hello");

            assertThat(deadLetterCount.get()).isEqualTo(1);
        }
    }

    @Test
    void testDeadLetterHandlerNotInvokedWhenSubscriberExists() {
        AtomicInteger deadLetterCount = new AtomicInteger(0);

        try (EventBus bus = EventBusBuilder.create()
                .withDeadLetterHandler(event -> deadLetterCount.incrementAndGet())
                .build()) {

            bus.subscribe("live/channel", String.class, e -> {});
            bus.publish("live/channel", "hello");

            assertThat(deadLetterCount.get()).isZero();
        }
    }

    @Test
    void testDeadLetterHandlerExceptionDoesNotPropagate() {
        try (EventBus bus = EventBusBuilder.create()
                .withDeadLetterHandler(event -> { throw new RuntimeException("dead-letter boom"); })
                .build()) {

            assertThatCode(() -> bus.publish("no/subscribers", "payload"))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void testBusWithoutDeadLetterHandlerDropsEventsQuietly() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            assertThatCode(() -> bus.publish("empty/channel", "payload"))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void testDeadLetterHandlerReceivesTheOriginalEvent() {
        Channel target = Channel.of("specific", "channel");
        AtomicInteger matchCount = new AtomicInteger(0);

        try (EventBus bus = EventBusBuilder.create()
                .withDeadLetterHandler(event -> {
                    if (event.getChannels().contains(target)) {
                        matchCount.incrementAndGet();
                    }
                })
                .build()) {

            bus.publish(target, "payload");

            assertThat(matchCount.get()).isEqualTo(1);
        }
    }
}
