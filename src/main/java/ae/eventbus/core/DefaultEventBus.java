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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class DefaultEventBus implements EventBus {

    private final EventRouter router;
    private final EventDispatcher dispatcher;
    private final EventRegistry registry;
    private final EventValidator validator;
    private final String channelSeparator;
    private final ReferenceType defaultReferenceType;
    private final Duration defaultTimeout;
    private final EventHandler<Event> deadLetterHandler;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final CopyOnWriteArrayList<Subscription> activeSubscriptions = new CopyOnWriteArrayList<>();

    public DefaultEventBus(EventRouter router,
                           EventDispatcher dispatcher,
                           EventRegistry registry,
                           EventValidator validator,
                           String channelSeparator,
                           ReferenceType defaultReferenceType,
                           Duration defaultTimeout,
                           EventHandler<Event> deadLetterHandler) {
        this.router = Objects.requireNonNull(router, "EventRouter must not be null.");
        this.dispatcher = Objects.requireNonNull(dispatcher, "EventDispatcher must not be null.");
        this.registry = Objects.requireNonNull(registry, "EventRegistry must not be null.");
        this.validator = Objects.requireNonNull(validator, "EventValidator must not be null.");
        this.channelSeparator = Objects.requireNonNull(channelSeparator, "ChannelSeparator must not be null.");
        this.defaultReferenceType = Objects.requireNonNull(defaultReferenceType, "DefaultReferenceType must not be null.");
        this.defaultTimeout = Objects.requireNonNull(defaultTimeout, "DefaultTimeout must not be null.");
        this.deadLetterHandler = deadLetterHandler; // nullable
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
            } catch (Throwable ignored) {
                // Dead-letter handler exceptions must never propagate to the publisher.
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
        ensureNotClosed();
        Channel channel = Channel.parse(channelPath, channelSeparator);
        publish(channel, payload);
    }

    @Override
    public <T> Subscription subscribe(Channel channel, Class<T> eventType, EventHandler<T> handler) {
        return subscribe(channel, eventType, Priority.NORMAL, DispatchMode.SYNCHRONOUS,
                defaultReferenceType, false, null, handler);
    }

    @Override
    public <T> Subscription subscribeToHierarchy(Channel channel, Class<T> eventType, EventHandler<T> handler) {
        return subscribe(channel, eventType, Priority.NORMAL, DispatchMode.SYNCHRONOUS,
                defaultReferenceType, true, null, handler);
    }

    @Override
    public <T> Subscription subscribe(Channel channel,
                                      Class<T> eventType,
                                      Priority priority,
                                      DispatchMode dispatchMode,
                                      ReferenceType referenceType,
                                      boolean handlesChannelChildren,
                                      EventHandler<T> handler) {
        return subscribe(channel, eventType, priority, dispatchMode, referenceType,
                handlesChannelChildren, null, handler);
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
        Objects.requireNonNull(priority, "Priority cannot be null.");
        Objects.requireNonNull(dispatchMode, "DispatchMode cannot be null.");
        Objects.requireNonNull(referenceType, "ReferenceType cannot be null.");
        Objects.requireNonNull(handler, "Handler cannot be null.");

        registry.registerChannel(channel);

        AtomicReference<Subscriber<T>> holderRef = new AtomicReference<>();
        Subscriber<T> subscriber = new Subscriber<>(
                channel, eventType, priority, dispatchMode, referenceType,
                handlesChannelChildren, handler, () -> router.deregister(holderRef.get()),
                null, subscriberTarget
        );
        holderRef.set(subscriber);
        router.register(subscriber);

        Subscription sub = subscriber.getSubscription();
        activeSubscriptions.add(sub);
        return sub;
    }

    // === REQUEST-RESPONSE IMPLEMENTATION ===

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <REQ, RES> Subscription handleRequest(Channel channel, Class<REQ> requestType, RequestHandler<REQ, RES> handler) {
        ensureNotClosed();
        Objects.requireNonNull(handler, "RequestHandler cannot be null.");
        Objects.requireNonNull(requestType, "RequestType cannot be null.");
        validator.validateChannel(channel);

        registry.registerChannel(channel);

        AtomicReference<Subscriber<RequestEnvelope>> holderRef = new AtomicReference<>();
        Subscriber<RequestEnvelope> subscriber = new Subscriber<>(
                channel,
                RequestEnvelope.class,
                Priority.NORMAL,
                DispatchMode.SYNCHRONOUS,
                defaultReferenceType,
                false,
                (EventHandler<RequestEnvelope>) envelope -> {
                    if (requestType.isInstance(envelope.getRequest())) {
                        try {
                            REQ req = requestType.cast(envelope.getRequest());
                            RES res = handler.handle(req);
                            envelope.submitResponse(res);
                        } catch (Throwable t) {
                            envelope.submitFailure(t);
                        }
                    }
                },
                () -> router.deregister(holderRef.get()),
                event -> (event instanceof RequestEnvelope<?, ?> req && requestType.isInstance(req.getRequest())),
                null
        );
        holderRef.set(subscriber);
        router.register(subscriber);

        Subscription sub = subscriber.getSubscription();
        activeSubscriptions.add(sub);
        return sub;
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

    private <REQ, RES> RequestEnvelope<REQ, RES> dispatchRequest(Channel channel, REQ request,
                                                                  Class<RES> responseType,
                                                                  UUID targetSubscriberId,
                                                                  Duration timeout) {
        ensureNotClosed();
        validator.validatePayload(channel, request);
        registry.registerChannel(channel);

        RequestEnvelope<REQ, RES> envelope = new RequestEnvelope<>(request, channel, responseType, targetSubscriberId);

        List<Subscriber<?>> matched = router.resolveSubscribers(envelope, channel);

        if (targetSubscriberId != null) {
            matched = matched.stream().filter(sub -> sub.getId().equals(targetSubscriberId)).toList();
        }

        if (matched.isEmpty()) {
            NoResponderException ex = new NoResponderException(channel);
            envelope.getSingleFuture().completeExceptionally(ex);
            envelope.getAllFuture().completeExceptionally(ex);
            return envelope;
        }

        envelope.setExpectedResponders(matched.size());
        envelope.getSingleFuture().orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
        envelope.getAllFuture().orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);

        registry.recordPublication(envelope, channel, matched.size());
        dispatcher.dispatch(envelope, matched);

        return envelope;
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

    @Override
    public EventRegistry getRegistry() {
        return registry;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (Subscription sub : activeSubscriptions) {
                try {
                    sub.unsubscribe();
                } catch (Throwable ignored) {
                    // Best-effort cleanup: the subscription is already inactive.
                }
            }
            activeSubscriptions.clear();
            try {
                dispatcher.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to shut down EventBus dispatcher.", e);
            }
        }
    }
}
