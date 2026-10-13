package ae.eventbus.core.registry;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;

/**
 * Immutable audit record of a single event publication.
 * Stores only metadata — no reference to the original event object — so that
 * sensitive payloads carried by the event are not retained in history.
 */
public final class EventRecord {

    private final UUID eventId;
    private final String eventType;
    private final Channel channel;
    private final Instant publishedAt;
    private final int recipientCount;

    public EventRecord(Event event, Channel channel, int recipientCount) {
        Objects.requireNonNull(event, "Event must not be null.");
        this.eventId = event.getId();
        this.eventType = event.getClass().getSimpleName();
        this.channel = Objects.requireNonNull(channel, "Channel must not be null.");
        this.publishedAt = Instant.now();
        this.recipientCount = recipientCount;
    }

    /** The UUID of the original event. */
    public UUID getEventId() {
        return eventId;
    }

    /** The simple class name of the original event (e.g. {@code "OrderCreatedEvent"}). */
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
        return String.format("EventRecord[id=%s, type=%s, channel=%s, publishedAt=%s, recipients=%d]",
                eventId, eventType, channel, publishedAt, recipientCount);
    }
}
