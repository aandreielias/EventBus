package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.registry.EventRecord;
import ae.eventbus.core.registry.EventRegistry;
import ae.eventbus.core.registry.SubscriberInfo;
import ae.eventbus.core.request.NoResponderException;
import ae.eventbus.core.request.RequestHandler;
import ae.eventbus.core.subscription.DispatchMode;
import ae.eventbus.core.subscription.EventHandler;
import ae.eventbus.core.subscription.Priority;
import ae.eventbus.core.subscription.ReferenceType;
import ae.eventbus.core.subscription.Subscription;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Core facade for the EventBus library.
 *
 * <p>Provides publish/subscribe, hierarchical routing, weak-reference subscriptions,
 * request-response, and introspection — all accessible through a single interface.</p>
 *
 * <p>Instances are created via {@link EventBusBuilder}. Always close the bus (try-with-resources
 * or explicit {@link #close()}) to release executor threads, fail pending requests and
 * deregister all subscriptions.</p>
 *
 * <p>All methods taking a {@code String} channel path parse it with the bus's configured
 * separator (see {@link #getChannelSeparator()}). Every method except the introspection
 * getters throws {@link IllegalStateException} once the bus is closed.</p>
 */
public interface EventBus extends AutoCloseable {

    // 1. PUBLISH

    /**
     * Publishes an event to every channel it is addressed to.
     * If no subscriber matches, the configured dead-letter handler (if any) receives the event.
     *
     * @param event the event to publish
     * @throws ae.eventbus.core.validation.ValidationException if the event fails validation
     */
    void publish(Event event);

    /**
     * Wraps {@code payload} in an {@link ae.eventbus.core.event.EventEnvelope} and publishes it.
     * Subscribers receive the payload itself, matched by its runtime type.
     */
    <T> void publish(Channel channel, T payload);

    /** String-path variant of {@link #publish(Channel, Object)}. */
    <T> void publish(String channelPath, T payload);

    // 2. SUBSCRIBE

    /**
     * Subscribes a handler to payloads of {@code eventType} published exactly on {@code channel},
     * with {@link Priority#NORMAL}, {@link DispatchMode#SYNCHRONOUS} and a strong reference.
     *
     * @return the subscription; close it to stop receiving events
     */
    <T> Subscription subscribe(Channel channel, Class<T> eventType, EventHandler<T> handler);

    /**
     * Like {@link #subscribe(Channel, Class, EventHandler)}, but also receives events published on
     * descendant channels (requires a hierarchical router).
     */
    <T> Subscription subscribeToHierarchy(Channel channel, Class<T> eventType, EventHandler<T> handler);

    /**
     * Fully configurable subscription.
     *
     * @param channel                channel to listen on
     * @param eventType              payload (or event) type to accept
     * @param priority               invocation order relative to other subscribers
     * @param dispatchMode           run on the publisher thread or on the async executor
     * @param referenceType          {@link ReferenceType#WEAK} ends the subscription once {@code subscriberTarget} is garbage-collected
     * @param handlesChannelChildren whether descendant-channel events are delivered too
     * @param subscriberTarget       owner object for {@code WEAK} subscriptions; required for {@code WEAK}, ignored for {@code STRONG}
     * @param handler                the handler
     * @throws IllegalArgumentException if {@code referenceType} is {@code WEAK} and {@code subscriberTarget} is {@code null},
     *                                  or if {@code eventType} is {@code RequestEnvelope} (use {@link #handleRequest})
     */
    <T> Subscription subscribe(Channel channel,
                               Class<T> eventType,
                               Priority priority,
                               DispatchMode dispatchMode,
                               ReferenceType referenceType,
                               boolean handlesChannelChildren,
                               Object subscriberTarget,
                               EventHandler<T> handler);

    /** Fully configurable subscription without a subscriber target (so {@code referenceType} must be {@code STRONG}). */
    default <T> Subscription subscribe(Channel channel,
                                       Class<T> eventType,
                                       Priority priority,
                                       DispatchMode dispatchMode,
                                       ReferenceType referenceType,
                                       boolean handlesChannelChildren,
                                       EventHandler<T> handler) {
        return subscribe(channel, eventType, priority, dispatchMode, referenceType, handlesChannelChildren, null, handler);
    }

    /** Weak subscription whose lifetime is tied to {@code subscriberTarget}. */
    default <T> Subscription subscribe(Channel channel, Class<T> eventType, Object subscriberTarget, EventHandler<T> handler) {
        return subscribe(channel, eventType, Priority.NORMAL, DispatchMode.SYNCHRONOUS,
                ReferenceType.WEAK, false, subscriberTarget, handler);
    }

    /**
     * Subscribes with a weak reference to {@code target}.
     * The handler runs as long as {@code target} is reachable; it is silently deregistered once GC'd.
     * Use this to avoid memory leaks when subscribing with method references on short-lived objects.
     * The handler receives the target as an argument, so it never needs to capture it.
     */
    default <T, TARGET> Subscription subscribeWeak(Channel channel, Class<T> eventType, TARGET target,
                                                    BiConsumer<TARGET, T> handler) {
        Objects.requireNonNull(target, "Target must not be null.");
        Objects.requireNonNull(handler, "Handler must not be null.");
        WeakReference<TARGET> ref = new WeakReference<>(target);
        return subscribe(channel, eventType, Priority.NORMAL, DispatchMode.SYNCHRONOUS,
                ReferenceType.WEAK, false, target, event -> {
                    TARGET t = ref.get();
                    if (t != null) handler.accept(t, event);
                });
    }

    /** String-path variant of {@link #subscribeWeak(Channel, Class, Object, BiConsumer)}. */
    default <T, TARGET> Subscription subscribeWeak(String channelPath, Class<T> eventType, TARGET target,
                                                    BiConsumer<TARGET, T> handler) {
        return subscribeWeak(Channel.parse(channelPath, getChannelSeparator()), eventType, target, handler);
    }

    /** Subscribes to any payload on the given string path. */
    default Subscription subscribe(String channelPath, EventHandler<Object> handler) {
        return subscribe(Channel.parse(channelPath, getChannelSeparator()), Object.class, handler);
    }

    /** Type-safe subscription using a string path. */
    default <T> Subscription subscribe(String channelPath, Class<T> eventType, EventHandler<T> handler) {
        return subscribe(Channel.parse(channelPath, getChannelSeparator()), eventType, handler);
    }

    /** Hierarchical subscription using a string path — the handler receives events from all descendant channels too. */
    default Subscription subscribeHierarchy(String channelPath, EventHandler<Object> handler) {
        return subscribeToHierarchy(Channel.parse(channelPath, getChannelSeparator()), Object.class, handler);
    }

    /** Type-safe hierarchical subscription using a string path. */
    default <T> Subscription subscribeHierarchy(String channelPath, Class<T> eventType, EventHandler<T> handler) {
        return subscribeToHierarchy(Channel.parse(channelPath, getChannelSeparator()), eventType, handler);
    }

    /**
     * Looks up a live subscription (including request handlers) by its id.
     *
     * @return the subscription, or empty if it never existed or has ended
     */
    Optional<Subscription> getSubscription(UUID subscriptionId);

    /**
     * Ends the subscription with the given id, if it is still active.
     *
     * @return {@code true} if a live subscription was found and ended
     */
    default boolean unsubscribe(UUID subscriptionId) {
        Optional<Subscription> subscription = getSubscription(subscriptionId);
        subscription.ifPresent(Subscription::unsubscribe);
        return subscription.isPresent();
    }

    // 3. REQUEST-RESPONSE

    /**
     * Registers a handler that answers requests of {@code requestType} on {@code channel}.
     * Handlers run on the async executor (so the requester's timeout always applies, and
     * thread-locals of the requesting thread are not visible) and are held strongly.
     */
    <REQ, RES> Subscription handleRequest(Channel channel, Class<REQ> requestType, RequestHandler<REQ, RES> handler);

    /** String-path variant of {@link #handleRequest(Channel, Class, RequestHandler)}. */
    default <REQ, RES> Subscription handleRequest(String channelPath, Class<REQ> requestType, RequestHandler<REQ, RES> handler) {
        return handleRequest(Channel.parse(channelPath, getChannelSeparator()), requestType, handler);
    }

    /**
     * Sends a request and completes with the first successful response. It fails only if every
     * matching handler fails, with the first failure as cause.
     * The future fails with {@link NoResponderException} if nobody handles {@code request}'s type,
     * with {@link java.util.concurrent.TimeoutException} after {@code timeout}, with
     * {@link ClassCastException} if the handler's response is not a {@code responseType}, and with
     * {@link IllegalStateException} if the bus closes first.
     */
    <REQ, RES> CompletableFuture<RES> request(Channel channel, REQ request, Class<RES> responseType, Duration timeout);

    /**
     * Scatter-gather: completes with the responses of every matching handler. Handlers that fail
     * are left out; the future fails only if all of them fail. Other failure rules as in
     * {@link #request(Channel, Object, Class, Duration)}.
     */
    <REQ, RES> CompletableFuture<List<RES>> requestAll(Channel channel, REQ request, Class<RES> responseType, Duration timeout);

    /** Sends a request to exactly one handler, identified by its subscription id. */
    <REQ, RES> CompletableFuture<RES> requestFrom(UUID subscriberId, Channel channel, REQ request, Class<RES> responseType, Duration timeout);

    /** {@link #request(Channel, Object, Class, Duration)} with the bus's default timeout. */
    <REQ, RES> CompletableFuture<RES> request(Channel channel, REQ request, Class<RES> responseType);

    /** {@link #requestAll(Channel, Object, Class, Duration)} with the bus's default timeout. */
    <REQ, RES> CompletableFuture<List<RES>> requestAll(Channel channel, REQ request, Class<RES> responseType);

    /** {@link #requestFrom(UUID, Channel, Object, Class, Duration)} with the bus's default timeout. */
    <REQ, RES> CompletableFuture<RES> requestFrom(UUID subscriberId, Channel channel, REQ request, Class<RES> responseType);

    default <REQ, RES> CompletableFuture<RES> request(String channelPath, REQ request, Class<RES> responseType) {
        return request(Channel.parse(channelPath, getChannelSeparator()), request, responseType);
    }

    default <REQ, RES> CompletableFuture<RES> request(String channelPath, REQ request, Class<RES> responseType, Duration timeout) {
        return request(Channel.parse(channelPath, getChannelSeparator()), request, responseType, timeout);
    }

    default <REQ, RES> CompletableFuture<List<RES>> requestAll(String channelPath, REQ request, Class<RES> responseType) {
        return requestAll(Channel.parse(channelPath, getChannelSeparator()), request, responseType);
    }

    default <REQ, RES> CompletableFuture<List<RES>> requestAll(String channelPath, REQ request, Class<RES> responseType, Duration timeout) {
        return requestAll(Channel.parse(channelPath, getChannelSeparator()), request, responseType, timeout);
    }

    default <REQ, RES> CompletableFuture<RES> requestFrom(UUID subscriberId, String channelPath, REQ request, Class<RES> responseType) {
        return requestFrom(subscriberId, Channel.parse(channelPath, getChannelSeparator()), request, responseType);
    }

    // 4. REGISTRY INTROSPECTION SHORTCUTS

    /** Returns the registry holding channel statistics, subscriber metadata and publication history. */
    EventRegistry getRegistry();

    /**
     * Returns the channel separator configured for this bus (e.g. {@code "/"} or {@code "."}).
     * Used by all string-path methods to parse channel paths correctly.
     */
    String getChannelSeparator();

    /** Returns {@code true} once {@link #close()} has been called. */
    boolean isClosed();

    /** Channels that currently have at least one subscriber. */
    default Set<Channel> getChannels() {
        return getRegistry().getActiveChannels();
    }

    /** Metadata of every live subscriber. */
    default List<SubscriberInfo> getSubscribers() {
        return getRegistry().getAllSubscribers();
    }

    /** Metadata of the live subscribers registered exactly on the given channel. */
    default List<SubscriberInfo> getSubscribers(String channelPath) {
        return getRegistry().getSubscribersForChannel(Channel.parse(channelPath, getChannelSeparator()));
    }

    /** Publication history, oldest first. */
    default List<EventRecord> getHistory() {
        return getRegistry().getHistory();
    }

    /** Publication history of one channel, oldest first. */
    default List<EventRecord> getHistory(String channelPath) {
        return getRegistry().getHistoryForChannel(Channel.parse(channelPath, getChannelSeparator()));
    }

    /**
     * Closes the bus: fails pending requests, ends every subscription and shuts down the
     * dispatcher (waiting up to 5 seconds for in-flight async handlers). Idempotent.
     */
    @Override
    void close();
}
