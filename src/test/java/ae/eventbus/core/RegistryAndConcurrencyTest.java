package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.registry.ChannelInfo;
import ae.eventbus.core.registry.DefaultEventRegistry;
import ae.eventbus.core.registry.EventRecord;
import ae.eventbus.core.registry.NoOpEventRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RegistryAndConcurrencyTest {

    @Test
    void testNoOpRegistryNeverReturnsNull() {
        NoOpEventRegistry registry = new NoOpEventRegistry();

        assertThat(registry.getAllChannelInfos()).isNotNull().isEmpty();
        assertThat(registry.getActiveChannels()).isNotNull().isEmpty();
        assertThat(registry.getAllSubscribers()).isNotNull().isEmpty();
        assertThat(registry.getHistory()).isNotNull().isEmpty();
        assertThat(registry.getHistoryForChannel(Channel.of("test"))).isNotNull().isEmpty();
        assertThat(registry.getChannelInfo(Channel.of("test"))).isEmpty();
    }

    @Test
    void testBoundedHistoryMaintainsMaxSize() {
        int maxSize = 50;
        try (EventBus bus = EventBusBuilder.create()
                .withMaxHistorySize(maxSize)
                .build()) {

            bus.subscribe("audit/events", String.class, e -> {});

            for (int i = 0; i < 200; i++) {
                bus.publish("audit/events", "Event-" + i);
            }

            List<EventRecord> history = bus.getHistory();
            assertThat(history).hasSize(maxSize);
        }
    }

    @Test
    void testConcurrentPublicationsStress() throws InterruptedException {
        int threadCount = 10;
        int eventsPerThread = 500;
        int maxHistory = 100;

        try (EventBus bus = EventBusBuilder.create()
                .withMaxHistorySize(maxHistory)
                .build()) {

            Channel channel = Channel.of("stress", "test");
            bus.subscribe(channel, Integer.class, val -> {});

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);

            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < eventsPerThread; i++) {
                            bus.publish(channel, i);
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();

            List<EventRecord> history = bus.getHistory();
            assertThat(history).hasSizeLessThanOrEqualTo(maxHistory);

            Optional<ChannelInfo> info = bus.getRegistry().getChannelInfo(channel);
            assertThat(info).isPresent();
            assertThat(info.get().getPublicationCount()).isEqualTo((long) threadCount * eventsPerThread);
        }
    }
}
