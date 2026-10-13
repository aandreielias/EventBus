package ae.eventbus.core.registry;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.event.EventEnvelope;
import ae.eventbus.core.request.RequestEnvelope;

/**
 * Immutable audit record of a single event publication.
 * Stores only metadata — no reference to the original event object — so that
 * sensitive payloads carried by the event are not retained in history.
 */
public final class EventRecord {

    private final long sequence;
    private final UUID eventId;
    private final String eventType;
    private final Channel channel;
    private final Instant publishedAt;
    private final int recipientCount;

    /**
     * @param sequence       monotonically increasing position of this record within its registry
     * @param event          the published event (only its metadata is retained)
     * @param channel        the channel the event was published on
     * @param recipientCount number of subscribers the event was dispatched to
     */
    public EventRecord(long sequence, Event event, Channel channel, int recipientCount) {
        Objects.requireNonNull(event, "Event must not be null.");
        this.sequence = sequence;
        this.eventId = event.getId();
        this.eventType = describeType(event);
        this.channel = Objects.requireNonNull(channel, "Channel must not be null.");
        this.publishedAt = Instant.now();
        this.recipientCount = recipientCount;
    }

    /**
     * Monotonically increasing sequence number assigned by the registry.
     * Survives {@link EventRegistry#clearHistory()}, so it can be used as a cursor for incremental polling.
     */
    public long getSequence() {
        return sequence;
    }

    /** The UUID of the original event. */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * The simple type name of what was published: the event class (e.g. {@code "OrderCreatedEvent"}),
     * the payload class for wrapped payloads (e.g. {@code "String"}), or {@code "Request<Query>"} for requests.
     */
    public String getEventType() {
        return eventType;
    }

    public Channel getChannel() {
        return channel;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public int getRecipientCount() {
        return recipientCount;
    }

    @Override
    public String toString() {
        return String.format("EventRecord[seq=%d, id=%s, type=%s, channel=%s, publishedAt=%s, recipients=%d]",
                sequence, eventId, eventType, channel, publishedAt, recipientCount);
    }

    private static String describeType(Event event) {
        if (event instanceof EventEnvelope<?> envelope) {
            return envelope.getPayloadType().getSimpleName();
        }
        if (event instanceof RequestEnvelope<?, ?> request) {
            return "Request<" + request.getRequest().getClass().getSimpleName() + ">";
        }
        return event.getClass().getSimpleName();
    }
}
