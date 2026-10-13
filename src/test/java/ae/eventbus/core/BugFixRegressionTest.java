package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.EventEnvelope;
import ae.eventbus.core.registry.SubscriberInfo;
import ae.eventbus.core.request.RequestEnvelope;
import ae.eventbus.core.subscription.DispatchMode;
import ae.eventbus.core.subscription.Priority;
import ae.eventbus.core.subscription.ReferenceType;
import ae.eventbus.core.subscription.Subscription;
import ae.eventbus.core.validation.ValidationException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * One test per defect found in the 1.0.0 review, so each fix stays fixed.
 */
class BugFixRegressionTest {

    @Test
    void unsubscribedSubscriptionsAreRemovedFromTheBusIndex() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            Subscription subscription = bus.subscribe("churn/channel", String.class, e -> {});
            UUID id = subscription.getId();
            assertThat(bus.getSubscription(id)).contains(subscription);

            subscription.unsubscribe();

            assertThat(bus.getSubscription(id)).isEmpty();
        }
    }

    @Test
    void unsubscribeByIdEndsTheSubscription() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            Subscription subscription = bus.subscribe("by/id", String.class, e -> {});

            assertThat(bus.unsubscribe(subscription.getId())).isTrue();
            assertThat(subscription.isActive()).isFalse();
            assertThat(bus.unsubscribe(subscription.getId())).isFalse();
        }
    }

    @Test
    void weakSubscriptionWithoutTargetIsRejected() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            assertThatThrownBy(() -> bus.subscribe(Channel.of("weak"), String.class, Priority.NORMAL,
                    DispatchMode.SYNCHRONOUS, ReferenceType.WEAK, false, e -> {}))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("subscriber target");
        }
    }

    @Test
    void handlersSurviveGarbageCollection() throws Exception {
        try (EventBus bus = EventBusBuilder.create().build()) {
            AtomicInteger hits = new AtomicInteger();
            bus.subscribe("gc/events", String.class, e -> hits.incrementAndGet());
            bus.handleRequest("gc/requests", String.class, req -> "pong");

            System.gc();

            bus.publish("gc/events", "hello");
            assertThat(hits.get()).isEqualTo(1);
            assertThat(bus.request("gc/requests", "ping", String.class).get(2, TimeUnit.SECONDS)).isEqualTo("pong");
        }
    }

    @Test
    void mistypedResponseFailsTheRequestWithClassCastException() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            bus.handleRequest("typed/request", String.class, req -> "not-a-number");

            CompletableFuture<Integer> future = bus.request("typed/request", "x", Integer.class, Duration.ofSeconds(2));

            assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(ClassCastException.class);
        }
    }

    @Test
    void concurrentSubscribeAndUnsubscribeNeverLosesASubscriber() throws Exception {
        try (EventBus bus = EventBusBuilder.create().build()) {
            Channel channel = Channel.of("race", "channel");
            int threads = 8;
            int iterations = 2_000;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try {
                List<Future<Integer>> results = new CopyOnWriteArrayList<>();
                for (int t = 0; t < threads; t++) {
                    results.add(pool.submit(() -> {
                        int lost = 0;
                        for (int i = 0; i < iterations; i++) {
                            Subscription s = bus.subscribe(channel, String.class, e -> {});
                            boolean routed = bus.getRegistry().getSubscribersForChannel(channel).stream()
                                    .map(SubscriberInfo::id)
                                    .anyMatch(s.getId()::equals);
                            if (!routed) lost++;
                            s.unsubscribe();
                        }
                        return lost;
                    }));
                }
                for (Future<Integer> result : results) {
                    assertThat(result.get(30, TimeUnit.SECONDS)).isZero();
                }
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void throwingExceptionHandlerDoesNotStopDeliveryOrReachThePublisher() {
        AtomicInteger secondSubscriberHits = new AtomicInteger();
        try (EventBus bus = EventBusBuilder.create()
                .withExceptionHandler((t, s, e) -> { throw new IllegalStateException("handler broke"); })
                .build()) {
            Channel channel = Channel.of("faulty");
            bus.subscribe(channel, String.class, Priority.HIGH, DispatchMode.SYNCHRONOUS, ReferenceType.STRONG, false,
                    e -> { throw new RuntimeException("subscriber broke"); });
            bus.subscribe(channel, String.class, Priority.LOW, DispatchMode.SYNCHRONOUS, ReferenceType.STRONG, false,
                    e -> secondSubscriberHits.incrementAndGet());

            assertThatCode(() -> bus.publish(channel, "x")).doesNotThrowAnyException();
            assertThat(secondSubscriberHits.get()).isEqualTo(1);
        }
    }

    @Test
    void reusedBuilderCreatesIndependentBuses() {
        EventBusBuilder builder = EventBusBuilder.create();
        AtomicInteger firstBusHits = new AtomicInteger();
        try (EventBus first = builder.build(); EventBus second = builder.build()) {
            first.subscribe("shared/name", String.class, e -> firstBusHits.incrementAndGet());

            second.publish("shared/name", "x");

            assertThat(firstBusHits.get()).isZero();
            assertThat(second.getSubscribers()).isEmpty();
        }
    }

    @Test
    void closingTheBusFailsPendingRequestsImmediately() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        EventBus bus = EventBusBuilder.create().withAsynchronousDispatching().build();
        bus.handleRequest("slow/request", String.class, req -> {
            release.await();
            return "late";
        });
        CompletableFuture<String> future = bus.request("slow/request", "x", String.class, Duration.ofMinutes(1));

        Thread closer = Thread.ofVirtual().start(bus::close);
        try {
            assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalStateException.class);
        } finally {
            release.countDown();
            closer.join();
        }
    }

    @Test
    void saturatedAsyncDispatchRunsOnThePublisherThread() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        List<Thread> handlerThreads = new CopyOnWriteArrayList<>();
        try (EventBus bus = EventBusBuilder.create()
                .withAsynchronousDispatching()
                .withMaxConcurrentAsyncTasks(1)
                .build()) {
            CountDownLatch firstStarted = new CountDownLatch(1);
            bus.subscribe("bounded", String.class, msg -> {
                handlerThreads.add(Thread.currentThread());
                if (msg.equals("first")) {
                    firstStarted.countDown();
                    release.await();
                }
            });

            bus.publish("bounded", "first");
            assertThat(firstStarted.await(2, TimeUnit.SECONDS)).isTrue();
            bus.publish("bounded", "second");

            assertThat(handlerThreads).hasSize(2);
            assertThat(handlerThreads.get(1)).isSameAs(Thread.currentThread());
        } finally {
            release.countDown();
        }
    }

    @Test
    void envelopeToStringDoesNotExposeThePayload() {
        EventEnvelope<String> envelope = EventEnvelope.of("member-secret-iban", Channel.of("members"));

        assertThat(envelope.toString()).doesNotContain("member-secret-iban").contains("String");
    }

    @Test
    void subscriberInfoReportsDeliveryAndFailureCounts() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            bus.subscribe("counted", String.class, msg -> {
                if (msg.equals("bad")) throw new IllegalArgumentException(msg);
            });

            bus.publish("counted", "ok");
            bus.publish("counted", "ok");
            bus.publish("counted", "bad");

            SubscriberInfo info = bus.getSubscribers("counted").get(0);
            assertThat(info.deliveredCount()).isEqualTo(2);
            assertThat(info.failedCount()).isEqualTo(1);
        }
    }

    @Test
    void historyRecordsCarryIncreasingSequenceNumbers() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            bus.publish("seq", "a");
            bus.publish("seq", "b");
            bus.getRegistry().clearHistory();
            bus.publish("seq", "c");

            assertThat(bus.getHistory()).singleElement()
                    .satisfies(record -> assertThat(record.getSequence()).isEqualTo(3));
        }
    }

    // === Second review ===

    @Test
    void oneFailingHandlerDoesNotFailTheRequestWhenAnotherAnswers() throws Exception {
        try (EventBus bus = EventBusBuilder.create().build()) {
            bus.handleRequest("q", String.class, req -> { throw new IllegalStateException("db down"); });
            bus.handleRequest("q", String.class, req -> "ok");

            assertThat(bus.request("q", "x", String.class).get(2, TimeUnit.SECONDS)).isEqualTo("ok");
            assertThat(bus.requestAll("q", "x", String.class).get(2, TimeUnit.SECONDS)).containsExactly("ok");
        }
    }

    @Test
    void requestFailsWhenEveryHandlerFails() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            bus.handleRequest("q", String.class, req -> { throw new IllegalStateException("first"); });
            bus.handleRequest("q", String.class, req -> { throw new IllegalStateException("second"); });

            assertThatThrownBy(() -> bus.request("q", "x", String.class).get(2, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> bus.requestAll("q", "x", String.class).get(2, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void requestTimeoutAppliesToSlowHandlers() {
        CountDownLatch release = new CountDownLatch(1);
        try (EventBus bus = EventBusBuilder.create().build()) {
            bus.handleRequest("slow", String.class, req -> {
                release.await();
                return "late";
            });

            long start = System.nanoTime();
            CompletableFuture<String> future = bus.request("slow", "x", String.class, Duration.ofMillis(100));

            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(100));
            assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TimeoutException.class);
        } finally {
            release.countDown();
        }
    }

    @Test
    void plainSubscribersCannotInterceptRequests() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            assertThatThrownBy(() -> bus.subscribeHierarchy("members", RequestEnvelope.class, env -> {}))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("handleRequest");
        }
    }

    @Test
    void equalPrioritySubscribersRunInRegistrationOrder() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            List<Integer> order = new CopyOnWriteArrayList<>();
            for (int i = 0; i < 20; i++) {
                int n = i;
                bus.subscribe("ordered", String.class, e -> order.add(n));
            }

            bus.publish("ordered", "x");

            assertThat(order).isSorted().hasSize(20);
        }
    }

    @Test
    void segmentsContainingTheSeparatorAreRejected() {
        try (EventBus bus = EventBusBuilder.create().withChannelSeparator(".").build()) {
            assertThatThrownBy(() -> bus.subscribe(Channel.of("app.services"), String.class, e -> {}))
                    .isInstanceOf(ValidationException.class);
            assertThatCode(() -> bus.subscribe("app.services", String.class, e -> {})).doesNotThrowAnyException();
        }
    }
}
