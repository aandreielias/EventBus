package ae.eventbus.core.admin;

import java.util.List;

/**
 * JMX management interface of an {@link ae.eventbus.core.EventBus}, implemented by {@link EventBusAdmin}.
 *
 * <p>All values are plain strings and numbers (timestamps are ISO-8601 instants), so the bean can be
 * read by any JMX client — {@code jconsole}, VisualVM, or the bundled {@code ae.eventbus.cli.EventBusCli}.
 * Channel paths are formatted and parsed with the bus's own separator. Payloads are never exposed.</p>
 */
public interface EventBusAdminMXBean {

    /** Bus-wide counters and configuration. */
    BusSummary getSummary();

    /** Every known channel (with or without subscribers), sorted by path. */
    List<ChannelView> getChannels();

    /** Every live subscriber, including request handlers. */
    List<SubscriberView> getSubscribers();

    /** Live subscribers registered exactly on {@code channelPath}. */
    List<SubscriberView> getSubscribersForChannel(String channelPath);

    /**
     * Publication records with a sequence number greater than {@code afterSequence}, oldest first.
     *
     * @param channelPath   only records on this channel or its descendants; {@code null} or empty for all
     * @param afterSequence cursor; pass {@code 0} for the whole retained history
     * @param limit         maximum number of records; the newest ones are kept
     */
    List<EventView> getHistory(String channelPath, long afterSequence, int limit);

    /**
     * Ends a subscription. Requires {@link AdminAccess#READ_WRITE}.
     *
     * @return {@code true} if a live subscription with that id existed
     * @throws SecurityException        if the bus is exposed read-only
     * @throws IllegalArgumentException if {@code subscriptionId} is not a UUID
     */
    boolean unsubscribe(String subscriptionId);

    /**
     * Clears the publication history. Requires {@link AdminAccess#READ_WRITE}.
     *
     * @throws SecurityException if the bus is exposed read-only
     */
    void clearHistory();

    /**
     * Bus-wide counters and configuration.
     *
     * @param name             name the bus was registered under
     * @param access           {@link AdminAccess} name
     * @param channelSeparator separator used in channel paths
     * @param closed           whether the bus has been closed
     * @param channelCount     number of known channels
     * @param subscriberCount  number of live subscribers
     * @param publicationCount total publications recorded across all channels
     * @param historySize      number of records currently retained
     * @param lastSequence     sequence number of the newest retained record, {@code 0} if none
     */
    record BusSummary(String name, String access, String channelSeparator, boolean closed,
                      int channelCount, int subscriberCount, long publicationCount,
                      int historySize, long lastSequence) {}

    /**
     * One channel and its activity.
     *
     * @param channel          channel path
     * @param subscriberCount  live subscribers registered exactly on this channel
     * @param publicationCount publications recorded on this channel
     * @param createdAt        ISO-8601 instant the channel became known, {@code null} if it has no statistics
     * @param lastFiredAt      ISO-8601 instant of the latest publication, {@code null} if never fired
     */
    record ChannelView(String channel, int subscriberCount, long publicationCount,
                       String createdAt, String lastFiredAt) {}

    /**
     * One live subscriber.
     *
     * @param id             subscription id (pass it to {@link #unsubscribe(String)})
     * @param channel        channel path the subscriber is registered on
     * @param eventType      simple name of the accepted type ({@code RequestEnvelope} for request handlers)
     * @param priority       {@link ae.eventbus.core.subscription.Priority} name
     * @param dispatchMode   {@link ae.eventbus.core.subscription.DispatchMode} name
     * @param referenceType  {@link ae.eventbus.core.subscription.ReferenceType} name
     * @param hierarchical   whether descendant-channel events are delivered too
     * @param deliveredCount events handled without throwing
     * @param failedCount    events for which the handler threw
     * @param subscribedAt   ISO-8601 instant of subscription
     */
    record SubscriberView(String id, String channel, String eventType, String priority,
                          String dispatchMode, String referenceType, boolean hierarchical,
                          long deliveredCount, long failedCount, String subscribedAt) {}

    /**
     * One publication record (metadata only).
     *
     * @param sequence       registry sequence number, usable as a polling cursor
     * @param eventId        id of the published event
     * @param eventType      event class, payload class or {@code Request<Type>}
     * @param channel        channel path
     * @param publishedAt    ISO-8601 instant of publication
     * @param recipientCount subscribers the event was dispatched to
     */
    record EventView(long sequence, String eventId, String eventType, String channel,
                     String publishedAt, int recipientCount) {}
}
