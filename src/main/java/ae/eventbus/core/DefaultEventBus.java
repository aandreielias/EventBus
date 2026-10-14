package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.dispatch.EventDispatcher;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.event.EventEnvelope;
import ae.eventbus.core.registry.EventRegistry;
import ae.eventbus.core.request.NoResponderException;
import ae.eventbus.core.request.RequestEnvelope;
import ae.eventbus.core.request.RequestHandler;
import ae.eventbus.core.routing.EventRouter;
import ae.eventbus.core.subscription.DispatchMode;
import ae.eventbus.core.subscription.EventHandler;
import ae.eventbus.core.subscription.Priority;
import ae.eventbus.core.subscription.ReferenceType;
import ae.eventbus.core.subscription.Subscriber;
import ae.eventbus.core.subscription.Subscription;
import ae.eventbus.core.validation.EventValidator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Default {@link EventBus} implementation: an orchestrator that delegates routing, dispatching,
 * validation and bookkeeping to the strategies supplied by {@link EventBusBuilder}.
 *
 * <p>Live subscriptions are indexed by id and removed from the index as soon as they end
 * (explicitly, through {@link #close()}, or because a weak target was collected), so
 * subscribe/unsubscribe churn does not accumulate memory. Pending requests are tracked
 * so that {@link #close()} can fail them instead of leaving callers waiting for a timeout.</p>
 */
public final class DefaultEventBus implements EventBus {

    private static final System.Logger LOGGER = System.getLogger(DefaultEventBus.class.getName());

    private final EventRouter router;
    private final EventDispatcher dispatcher;
    private final EventRegistry registry;
    private final EventValidator validator;
    private final String channelSeparator;
    private final Duration defaultTimeout;
    private final EventHandler<Event> deadLetterHandler;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ConcurrentHashMap<UUID, Subscription> subscriptions = new ConcurrentHashMap<>();
    private final Set<RequestEnvelope<?, ?>> pendingRequests = ConcurrentHashMap.newKeySet();

    /**
     * @param router            resolves subscribers for a channel
     * @param dispatcher        invokes resolved subscribers; closed together with the bus
     * @param registry          records channels and publication history
     * @param validator         validates channels, events and payloads
     * @param channelSeparator  separator used to parse string channel paths
     * @param defaultTimeout    timeout for requests that do not specify one
     * @param deadLetterHandler receives events nobody subscribed to; may be {@code null}
     */
    public DefaultEventBus(EventRouter router,
                           EventDispatcher dispatcher,
                           EventRegistry registry,
                           EventValidator validator,
                           String channelSeparator,
                           Duration defaultTimeout,
                           EventHandler<Event> deadLetterHandler) {
        this.router = Objects.requireNonNull(router, "EventRouter must not be null.");
        this.dispatcher = Objects.requireNonNull(dispatcher, "EventDispatcher must not be null.");
        this.registry = Objects.requireNonNull(registry, "EventRegistry must not be null.");
        this.validator = Objects.requireNonNull(validator, "EventValidator must not be null.");
        this.channelSeparator = Objects.requireNonNull(channelSeparator, "ChannelSeparator must not be null.");
        this.defaultTimeout = Objects.requireNonNull(defaultTimeout, "DefaultTimeout must not be null.");
        this.deadLetterHandler = deadLetterHandler;
    }

    private void ensureNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("EventBus has been closed.");
        }
    }

    @Override
    public String getChannelSeparator() {
        return channelSeparator;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    // === PUBLISH ===

    @Override
    public void publish(Event event) {
        ensureNotClosed();
        if (event == null) throw new IllegalArgumentException("Event cannot be null.");
        validator.validateEvent(event);

        Set<Subscriber<?>> allMatched = new LinkedHashSet<>();
        for (Channel channel : event.getChannels()) {
            registry.registerChannel(channel);
            List<Subscriber<?>> matched = router.resolveSubscribers(event, channel);
            registry.recordPublication(event, channel, matched.size());
            allMatched.addAll(matched);
        }

        if (!allMatched.isEmpty()) {
            List<Subscriber<?>> sorted = new ArrayList<>(allMatched);
            Collections.sort(sorted);
            dispatcher.dispatch(event, sorted);
        } else if (deadLetterHandler != null) {
            try {
                deadLetterHandler.handle(event);
            } catch (Throwable t) {
                // Dead-letter handler exceptions must never propagate to the publisher.
                LOGGER.log(System.Logger.Level.WARNING, "Dead-letter handler failed for event " + event.getId(), t);
            }
        }
    }

    @Override
    public <T> void publish(Channel channel, T payload) {
        ensureNotClosed();
        validator.validatePayload(channel, payload);
        publish(new EventEnvelope<>(payload, channel));
    }

    @Override
    public <T> void publish(String channelPath, T payload) {
        publish(Channel.parse(channelPath, channelSeparator), payload);
    }

    // === SUBSCRIBE ===

    @Override
    public <T> Subscription subscribe(Channel channel, Class<T> eventType, EventHandler<T> handler) {
        return subscribe(channel, eventType, Priority.NORMAL, DispatchMode.SYNCHRONOUS,
                ReferenceType.STRONG, false, null, handler);
    }

    @Override
    public <T> Subscription subscribeToHierarchy(Channel channel, Class<T> eventType, EventHandler<T> handler) {
        return subscribe(channel, eventType, Priority.NORMAL, DispatchMode.SYNCHRONOUS,
                ReferenceType.STRONG, true, null, handler);
    }

    @Override
    public <T> Subscription subscribe(Channel channel,
                                      Class<T> eventType,
                                      Priority priority,
                                      DispatchMode dispatchMode,
                                      ReferenceType referenceType,
                                      boolean handlesChannelChildren,
                                      Object subscriberTarget,
                                      EventHandler<T> handler) {
        ensureNotClosed();
        validator.validateChannel(channel);
        Objects.requireNonNull(eventType, "EventType cannot be null.");
        Objects.requireNonNull(handler, "Handler cannot be null.");
        if (RequestEnvelope.class.isAssignableFrom(eventType)) {
            throw new IllegalArgumentException("Requests can only be answered through handleRequest(...).");
        }

        return register(holder -> new Subscriber<>(
                channel, eventType, priority, dispatchMode, referenceType,
                handlesChannelChildren, handler, holder, null, subscriberTarget));
    }

    @Override
    public Optional<Subscription> getSubscription(UUID subscriptionId) {
        return Optional.ofNullable(subscriptions.get(Objects.requireNonNull(subscriptionId, "Subscription id must not be null.")));
    }

    /**
     * Creates, indexes and routes a subscriber. The factory receives the unsubscribe callback,
     * which removes the subscriber from both the router and the subscription index.
     */
    private <T> Subscription register(Function<Runnable, Subscriber<T>> factory) {
        AtomicReference<Subscriber<T>> holderRef = new AtomicReference<>();
        Subscriber<T> subscriber = factory.apply(() -> {
            Subscriber<T> self = holderRef.get();
            router.deregister(self);
            subscriptions.remove(self.getId());
        });
        holderRef.set(subscriber);

        registry.registerChannel(subscriber.getChannel());
        Subscription subscription = subscriber.getSubscription();
        subscriptions.put(subscription.getId(), subscription);
        router.register(subscriber);

        // close() may have run between ensureNotClosed() and the put above; don't leak past it.
        if (closed.get()) {
            subscription.unsubscribe();
            throw new IllegalStateException("EventBus has been closed.");
        }
        return subscription;
    }

    // === REQUEST-RESPONSE ===

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <REQ, RES> Subscription handleRequest(Channel channel, Class<REQ> requestType, RequestHandler<REQ, RES> handler) {
        ensureNotClosed();
        Objects.requireNonNull(handler, "RequestHandler cannot be null.");
        Objects.requireNonNull(requestType, "RequestType cannot be null.");
        validator.validateChannel(channel);

        EventHandler<RequestEnvelope> envelopeHandler = envelope -> {
            // Timed out or cancelled before delivery: nobody waits for the answer, so skip side effects.
            if (envelope.isDone()) {
                return;
            }
            try {
                envelope.submitResponse(handler.handle(requestType.cast(envelope.getRequest())));
            } catch (Throwable t) {
                envelope.submitFailure(t);
            }
        };
        Predicate<Event> acceptsRequestType =
                event -> event instanceof RequestEnvelope<?, ?> req && requestType.isInstance(req.getRequest());

        return register(holder -> new Subscriber<>(
                channel, RequestEnvelope.class, Priority.NORMAL, DispatchMode.ASYNCHRONOUS,
                ReferenceType.STRONG, false, envelopeHandler, holder, acceptsRequestType, null));
    }

    @Override
    public <REQ, RES> CompletableFuture<RES> request(Channel channel, REQ request, Class<RES> responseType, Duration timeout) {
        return dispatchRequest(channel, request, responseType, null, timeout).getSingleFuture();
    }

    @Override
    public <REQ, RES> CompletableFuture<List<RES>> requestAll(Channel channel, REQ request, Class<RES> responseType, Duration timeout) {
        return dispatchRequest(channel, request, responseType, null, timeout).getAllFuture();
    }

    @Override
    public <REQ, RES> CompletableFuture<RES> requestFrom(UUID subscriberId, Channel channel, REQ request, Class<RES> responseType, Duration timeout) {
        Objects.requireNonNull(subscriberId, "Target subscriberId must not be null.");
        return dispatchRequest(channel, request, responseType, subscriberId, timeout).getSingleFuture();
    }

    @Override
    public <REQ, RES> CompletableFuture<RES> request(Channel channel, REQ request, Class<RES> responseType) {
        return request(channel, request, responseType, defaultTimeout);
    }

    @Override
    public <REQ, RES> CompletableFuture<List<RES>> requestAll(Channel channel, REQ request, Class<RES> responseType) {
        return requestAll(channel, request, responseType, defaultTimeout);
    }

    @Override
    public <REQ, RES> CompletableFuture<RES> requestFrom(UUID subscriberId, Channel channel, REQ request, Class<RES> responseType) {
        return requestFrom(subscriberId, channel, request, responseType, defaultTimeout);
    }

    private <REQ, RES> RequestEnvelope<REQ, RES> dispatchRequest(Channel channel, REQ request,
                                                                  Class<RES> responseType,
                                                                  UUID targetSubscriberId,
                                                                  Duration timeout) {
        ensureNotClosed();
        Objects.requireNonNull(timeout, "Timeout must not be null.");
        validator.validatePayload(channel, request);
        registry.registerChannel(channel);

        RequestEnvelope<REQ, RES> envelope = new RequestEnvelope<>(request, channel, responseType, targetSubscriberId);

        List<Subscriber<?>> matched = router.resolveSubscribers(envelope, channel);
        if (targetSubscriberId != null) {
            matched = matched.stream().filter(sub -> sub.getId().equals(targetSubscriberId)).toList();
        }

        if (matched.isEmpty()) {
            envelope.cancel(new NoResponderException(channel));
            return envelope;
        }

        envelope.setExpectedResponders(matched.size());
        envelope.getSingleFuture().orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
        envelope.getAllFuture().orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);

        pendingRequests.add(envelope);
        CompletableFuture.allOf(envelope.getSingleFuture(), envelope.getAllFuture())
                .whenComplete((ignored, error) -> pendingRequests.remove(envelope));

        registry.recordPublication(envelope, channel, matched.size());
        dispatcher.dispatch(envelope, matched);

        return envelope;
    }

    // === INTROSPECTION & LIFECYCLE ===

    @Override
    public EventRegistry getRegistry() {
        return registry;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        IllegalStateException closedCause = new IllegalStateException("EventBus was closed before the request completed.");
        for (RequestEnvelope<?, ?> envelope : pendingRequests) {
            envelope.cancel(closedCause);
        }
        pendingRequests.clear();

        for (Subscription subscription : List.copyOf(subscriptions.values())) {
            try {
                subscription.unsubscribe();
            } catch (Throwable ignored) {
                // Best-effort cleanup: the subscription is already inactive.
            }
        }
        subscriptions.clear();

        try {
            dispatcher.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to shut down EventBus dispatcher.", e);
        }
    }
}
