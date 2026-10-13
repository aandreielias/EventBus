package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.registry.EventRecord;
import ae.eventbus.core.registry.EventRegistry;
import ae.eventbus.core.registry.SubscriberInfo;
import ae.eventbus.core.request.RequestHandler;
import ae.eventbus.core.subscription.DispatchMode;
import ae.eventbus.core.subscription.EventHandler;
import ae.eventbus.core.subscription.Priority;
import ae.eventbus.core.subscription.ReferenceType;
import ae.eventbus.core.subscription.Subscription;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Core facade for the EventBus library.
 *
 * <p>Provides publish/subscribe, hierarchical routing, weak-reference subscriptions,
 * request-response, and introspection — all accessible through a single interface.</p>
 *
 * <p>Instances are created via {@link EventBusBuilder}. Always close the bus (try-with-resources
 * or explicit {@link #close()}) to release executor threads and deregister all subscriptions.</p>
 */
public interface EventBus extends AutoCloseable {

    // 1. PUBLISH

    void publish(Event event);

    <T> void publish(Channel channel, T payload);

    <T> void publish(String channelPath, T payload);

    // 2. SUBSCRIBE

    <T> Subscription subscribe(Channel channel, Class<T> eventType, EventHandler<T> handler);

    <T> Subscription subscribeToHierarchy(Channel channel, Class<T> eventType, EventHandler<T> handler);

    <T> Subscription subscribe(Channel channel,
                               Class<T> eventType,
                               Priority priority,
                               DispatchMode dispatchMode,
                               ReferenceType referenceType,
                               boolean handlesChannelChildren,
                               Object subscriberTarget,
                               EventHandler<T> handler);

    default <T> Subscription subscribe(Channel channel,
                                       Class<T> eventType,
                                       Priority priority,
                                       DispatchMode dispatchMode,
                                       ReferenceType referenceType,
                                       boolean handlesChannelChildren,
                                       EventHandler<T> handler) {
        return subscribe(channel, eventType, priority, dispatchMode, referenceType, handlesChannelChildren, null, handler);
    }

    default <T> Subscription subscribe(Channel channel, Class<T> eventType, Object subscriberTarget, EventHandler<T> handler) {
        return subscribe(channel, eventType, Priority.NORMAL, DispatchMode.SYNCHRONOUS,
                ReferenceType.WEAK, false, subscriberTarget, handler);
    }

    /**
     * Subscribes with a weak reference to {@code target}.
     * The handler runs as long as {@code target} is reachable; it is silently deregistered once GC'd.
     * Use this to avoid memory leaks when subscribing with method references on short-lived objects.
     */
    default <T, TARGET> Subscription subscribeWeak(Channel channel, Class<T> eventType, TARGET target,
                                                    java.util.function.BiConsumer<TARGET, T> handler) {
        java.util.Objects.requireNonNull(target, "Target must not be null.");
        java.util.Objects.requireNonNull(handler, "Handler must not be null.");
        java.lang.ref.WeakReference<TARGET> ref = new java.lang.ref.WeakReference<>(target);
        return subscribe(channel, eventType, Priority.NORMAL, DispatchMode.SYNCHRONOUS,
                ReferenceType.WEAK, false, target, event -> {
                    TARGET t = ref.get();
                    if (t != null) handler.accept(t, event);
                });
    }

    default <T, TARGET> Subscription subscribeWeak(String channelPath, Class<T> eventType, TARGET target,
                                                    java.util.function.BiConsumer<TARGET, T> handler) {
        return subscribeWeak(Channel.parse(channelPath, getChannelSeparator()), eventType, target, handler);
    }

    /** Subscribes to any payload on the given string path using the bus's configured separator. */
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

    default <T> Subscription subscribeHierarchy(String channelPath, Class<T> eventType, EventHandler<T> handler) {
        return subscribeToHierarchy(Channel.parse(channelPath, getChannelSeparator()), eventType, handler);
    }

    // 3. REQUEST-RESPONSE

    <REQ, RES> Subscription handleRequest(Channel channel, Class<REQ> requestType, RequestHandler<REQ, RES> handler);

    default <REQ, RES> Subscription handleRequest(String channelPath, Class<REQ> requestType, RequestHandler<REQ, RES> handler) {
        return handleRequest(Channel.parse(channelPath, getChannelSeparator()), requestType, handler);
    }

    <REQ, RES> CompletableFuture<RES> request(Channel channel, REQ request, Class<RES> responseType, Duration timeout);

    <REQ, RES> CompletableFuture<List<RES>> requestAll(Channel channel, REQ request, Class<RES> responseType, Duration timeout);

    <REQ, RES> CompletableFuture<RES> requestFrom(UUID subscriberId, Channel channel, REQ request, Class<RES> responseType, Duration timeout);

    <REQ, RES> CompletableFuture<RES> request(Channel channel, REQ request, Class<RES> responseType);

    <REQ, RES> CompletableFuture<List<RES>> requestAll(Channel channel, REQ request, Class<RES> responseType);

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

    EventRegistry getRegistry();

    /**
     * Returns the channel separator configured for this bus (e.g. {@code "/"} or {@code "."}).
     * Used by all string-path methods to parse channel paths correctly.
     */
    String getChannelSeparator();

    default Set<Channel> getChannels() {
        return getRegistry().getActiveChannels();
    }

    default List<SubscriberInfo> getSubscribers() {
        return getRegistry().getAllSubscribers();
    }

    default List<SubscriberInfo> getSubscribers(String channelPath) {
        return getRegistry().getSubscribersForChannel(Channel.parse(channelPath, getChannelSeparator()));
    }

    default List<EventRecord> getHistory() {
        return getRegistry().getHistory();
    }

    default List<EventRecord> getHistory(String channelPath) {
        return getRegistry().getHistoryForChannel(Channel.parse(channelPath, getChannelSeparator()));
    }

    @Override
    void close();
}
