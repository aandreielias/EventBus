package ae.eventbus.core.event;

import java.util.Objects;
import java.util.Set;

import ae.eventbus.core.channel.Channel;

/**
 * Generic decorator/envelope that wraps any arbitrary payload object {@code T}
 * into a first-class {@link Event}.
 * <p>
 * This enables publishing third-party objects or raw data without forcing
 * payload classes to implement the {@link Event} interface directly.
 * </p>
 *
 * @param <T> the type of the payload wrapped inside this envelope
 */
public final class EventEnvelope<T> extends AbstractEvent {

    private final T payload;

    /**
     * Wraps a payload and binds it to a primary channel and optional additional channels.
     *
     * @param payload            the payload to wrap
     * @param primaryChannel     the primary destination channel
     * @param additionalChannels optional additional destination channels
     * @throws IllegalArgumentException if payload or primaryChannel is null
     */
    public EventEnvelope(T payload, Channel primaryChannel, Channel... additionalChannels) {
        super(primaryChannel, additionalChannels);
        this.payload = Objects.requireNonNull(payload, "Payload must not be null.");
    }

    /**
     * Wraps a payload and binds it to a set of channels.
     *
     * @param payload  the payload to wrap
     * @param channels the set of target channels
     * @throws IllegalArgumentException if payload or channels is null or empty
     */
    public EventEnvelope(T payload, Set<Channel> channels) {
        super(channels);
        this.payload = Objects.requireNonNull(payload, "Payload must not be null.");
    }
    
    /**
     * Static factory method for convenience.
     *
     * @param payload the data payload
     * @param channel the target channel
     * @param <T>     the payload type
     * @return a new {@link EventEnvelope} instance
     */
    public static <T> EventEnvelope<T> of(T payload, Channel channel) {
        return new EventEnvelope<>(payload, channel);
    }

    /**
     * Retrieves the wrapped payload.
     *
     * @return the payload instance
     */
    public T getPayload() {
        return payload;
    }

    /**
     * Returns the runtime class of the wrapped payload.
     * Useful for type-based routing and subscriber matching.
     *
     * @return payload class type
     */
    public Class<?> getPayloadType() {
        return payload.getClass();
    }

    /** Describes the envelope without the payload itself, so logging an envelope never leaks payload data. */
    @Override
    public String toString() {
        return String.format("EventEnvelope[id=%s, payloadType=%s, channels=%s]",
                getId(), payload.getClass().getSimpleName(), getChannels());
    }
}
