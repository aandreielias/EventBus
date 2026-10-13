package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.subscription.DispatchMode;
import ae.eventbus.core.subscription.Priority;
import ae.eventbus.core.subscription.ReferenceType;
import ae.eventbus.core.subscription.Subscription;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class WeakReferenceTest {

    static class ListenerTarget {
        final AtomicInteger count = new AtomicInteger(0);

        void onEvent(String msg) {
            count.incrementAndGet();
        }
    }

    @Test
    void testTargetBasedWeakSubscription() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            Channel channel = Channel.of("weak", "target");

            ListenerTarget target = new ListenerTarget();

            Subscription subscription = bus.subscribeWeak(
                    channel,
                    String.class,
                    target,
                    ListenerTarget::onEvent
            );

            bus.publish(channel, "message-1");
            assertThat(target.count.get()).isEqualTo(1);
            assertThat(subscription.isActive()).isTrue();

            // Clear target reference and force GC
            target = null;
            System.gc();
            System.runFinalization();
            System.gc();

            // Publish another message: will check isValid(), detect reclaimed target, and deregister
            bus.publish(channel, "message-2");

            assertThat(subscription.isActive()).isFalse();
        }
    }
}
