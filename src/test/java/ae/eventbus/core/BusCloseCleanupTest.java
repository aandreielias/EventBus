package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.subscription.Subscription;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link EventBus#close()} cleans up all active subscriptions
 * and that the bus cannot be used afterward.
 */
class BusCloseCleanupTest {

    @Test
    void testAllSubscriptionsInactiveAfterClose() {
        EventBus bus = EventBusBuilder.create().build();

        Subscription s1 = bus.subscribe("test/a", String.class, e -> {});
        Subscription s2 = bus.subscribe("test/b", String.class, e -> {});
        Subscription s3 = bus.subscribe("test/c", String.class, e -> {});

        assertThat(s1.isActive()).isTrue();
        assertThat(s2.isActive()).isTrue();
        assertThat(s3.isActive()).isTrue();

        bus.close();

        assertThat(s1.isActive()).isFalse();
        assertThat(s2.isActive()).isFalse();
        assertThat(s3.isActive()).isFalse();
    }

    @Test
    void testRouterEmptiedAfterClose() {
        EventBus bus = EventBusBuilder.create().build();
        Channel channel = Channel.of("router", "test");

        bus.subscribe(channel, String.class, e -> {});

        assertThat(bus.getChannels()).contains(channel);

        bus.close();

        assertThat(bus.getChannels()).doesNotContain(channel);
    }

    @Test
    void testCloseIsIdempotent() {
        EventBus bus = EventBusBuilder.create().build();
        bus.subscribe("idempotent/channel", String.class, e -> {});

        bus.close();
        bus.close(); // must not throw
    }
}
