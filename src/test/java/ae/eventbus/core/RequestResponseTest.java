package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.dispatch.EventDispatcher;
import ae.eventbus.core.request.NoResponderException;
import ae.eventbus.core.subscription.Subscription;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequestResponseTest {

    record PingRequest(String message) {}
    record PongResponse(String reply) {}

    record OrderQuery(long orderId) {}
    record OrderResult(String status) {}

    record CustomerQuery(long customerId) {}
    record CustomerResult(String name) {}

    @Test
    void testSingleRequestResponse() throws Exception {
        try (EventBus bus = EventBusBuilder.create().build()) {
            Channel channel = Channel.of("rpc", "ping");

            bus.handleRequest(channel, PingRequest.class, req -> new PongResponse("PONG: " + req.message()));

            CompletableFuture<PongResponse> future = bus.request(channel, new PingRequest("hello"), PongResponse.class);
            PongResponse response = future.get(2, TimeUnit.SECONDS);

            assertThat(response).isNotNull();
            assertThat(response.reply()).isEqualTo("PONG: hello");
        }
    }

    @Test
    void testNullResponseDoesNotHang() throws Exception {
        try (EventBus bus = EventBusBuilder.create().build()) {
            Channel channel = Channel.of("rpc", "nullable");

            bus.handleRequest(channel, String.class, req -> null);

            CompletableFuture<String> future = bus.request(channel, "test", String.class, Duration.ofSeconds(2));
            String result = future.get(2, TimeUnit.SECONDS);

            assertThat(result).isNull();
        }
    }

    @Test
    void testMultipleRequestTypesOnSameChannelDoNotHang() throws Exception {
        try (EventBus bus = EventBusBuilder.create().build()) {
            Channel channel = Channel.of("services", "gateway");

            // Register handler for OrderQuery
            bus.handleRequest(channel, OrderQuery.class, req -> new OrderResult("ACTIVE-" + req.orderId()));

            // Register handler for CustomerQuery on the SAME channel
            bus.handleRequest(channel, CustomerQuery.class, req -> new CustomerResult("Alice-" + req.customerId()));

            // Request OrderQuery: must NOT hang waiting for CustomerQuery handler!
            CompletableFuture<OrderResult> orderFuture = bus.request(channel, new OrderQuery(42), OrderResult.class, Duration.ofSeconds(2));
            OrderResult orderResult = orderFuture.get(2, TimeUnit.SECONDS);
            assertThat(orderResult.status()).isEqualTo("ACTIVE-42");

            // Request CustomerQuery: must NOT hang waiting for OrderQuery handler!
            CompletableFuture<CustomerResult> custFuture = bus.request(channel, new CustomerQuery(99), CustomerResult.class, Duration.ofSeconds(2));
            CustomerResult custResult = custFuture.get(2, TimeUnit.SECONDS);
            assertThat(custResult.name()).isEqualTo("Alice-99");
        }
    }

    @Test
    void testRequestAllMultiResponder() throws Exception {
        try (EventBus bus = EventBusBuilder.create().build()) {
            Channel channel = Channel.of("services", "auction");

            bus.handleRequest(channel, String.class, req -> "Bid-100");
            bus.handleRequest(channel, String.class, req -> "Bid-200");
            bus.handleRequest(channel, String.class, req -> "Bid-300");

            CompletableFuture<List<String>> future = bus.requestAll(channel, "item-1", String.class, Duration.ofSeconds(2));
            List<String> responses = future.get(2, TimeUnit.SECONDS);

            assertThat(responses).containsExactlyInAnyOrder("Bid-100", "Bid-200", "Bid-300");
        }
    }

    @Test
    void testTargetedRequestFrom() throws Exception {
        try (EventBus bus = EventBusBuilder.create().build()) {
            Channel channel = Channel.of("nodes", "compute");

            Subscription sub1 = bus.handleRequest(channel, String.class, req -> "Worker-1-Result");
            Subscription sub2 = bus.handleRequest(channel, String.class, req -> "Worker-2-Result");

            CompletableFuture<String> future = bus.requestFrom(sub2.getId(), channel, "job", String.class, Duration.ofSeconds(2));
            String result = future.get(2, TimeUnit.SECONDS);

            assertThat(result).isEqualTo("Worker-2-Result");
        }
    }

    @Test
    void testNoResponderThrowsException() {
        try (EventBus bus = EventBusBuilder.create().build()) {
            Channel channel = Channel.of("empty", "channel");

            CompletableFuture<String> future = bus.request(channel, "hello", String.class, Duration.ofSeconds(1));

            assertThatThrownBy(future::get)
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(NoResponderException.class);
        }
    }

    @Test
    void testHandlerIsSkippedWhenRequestTimedOutBeforeDispatch() throws Exception {
        // Dispatcher that delivers late, after the request has already timed out.
        CompletableFuture<Void> delivered = new CompletableFuture<>();
        EventDispatcher lateDispatcher = (event, subscribers) -> Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(200);
                for (var sub : subscribers) {
                    sub.invoke(event);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception ignored) {
                // Failures are irrelevant here; the test only checks whether the handler ran.
            } finally {
                delivered.complete(null);
            }
        });

        try (EventBus bus = EventBusBuilder.create().withDispatcher(lateDispatcher).build()) {
            Channel channel = Channel.of("rpc", "late");
            AtomicBoolean handlerRan = new AtomicBoolean(false);
            bus.handleRequest(channel, PingRequest.class, req -> {
                handlerRan.set(true);
                return new PongResponse("too late");
            });

            CompletableFuture<PongResponse> future =
                    bus.request(channel, new PingRequest("x"), PongResponse.class, Duration.ofMillis(50));

            assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TimeoutException.class);
            delivered.get(2, TimeUnit.SECONDS);
            assertThat(handlerRan).isFalse();
        }
    }
}
