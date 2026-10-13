package ae.eventbus.core.event;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import ae.eventbus.core.channel.Channel;

/**
 * Skeletal implementation of {@link Event} to reduce boilerplate for custom domain events.
 * Provides auto-generated ID, creation timestamp, channel management, and a deterministic
 * {@link #getPrimaryChannel()} backed by an explicit field.
 */
public class AbstractEvent implements Event {

    private final UUID id;
    private final Instant timestamp;
    private final Channel primaryChannel;
    private final Set<Channel> channels;

    /**
     * Constructs an event assigned to one or more channels.
     * The first argument is stored as the deterministic primary channel.
     *
     * @param primaryChannel    the main channel (non-null)
     * @param additionalChannels optional extra channels (nulls are silently ignored)
     */
    protected AbstractEvent(Channel primaryChannel, Channel... additionalChannels) {
        if (primaryChannel == null) {
            throw new IllegalArgumentException("Primary channel must not be null.");
        }
        this.id = UUID.randomUUID();
        this.timestamp = Instant.now();
        this.primaryChannel = primaryChannel;

        Set<Channel> set = new HashSet<>();
        set.add(primaryChannel);
        if (additionalChannels != null) {
            for (Channel channel : additionalChannels) {
                if (channel != null) {
                    set.add(channel);
                }
            }
        }
        this.channels = Collections.unmodifiableSet(set);
    }

    /**
     * Constructs an event with an explicit set of channels.
     * The primary channel is the first element returned by the set's iterator —
     * deterministic only if the set has a stable iteration order.
     *
     * @param channels set of target channels (non-null, non-empty, no null elements)
     */
    protected AbstractEvent(Set<Channel> channels) {
        if (channels == null || channels.isEmpty()) {
            throw new IllegalArgumentException("Event must belong to at least one channel.");
        }
        for (Channel ch : channels) {
            if (ch == null) {
                throw new IllegalArgumentException("Channel elements must not be null.");
            }
        }
        this.id = UUID.randomUUID();
        this.timestamp = Instant.now();
        this.channels = Set.copyOf(channels);
        this.primaryChannel = this.channels.iterator().next();
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public Set<Channel> getChannels() {
        return channels;
    }

    /** Returns the deterministic primary channel supplied at construction time. */
    @Override
    public Channel getPrimaryChannel() {
        return primaryChannel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Event other)) return false;
        return Objects.equals(id, other.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return String.format("%s[id=%s, timestamp=%s, channels=%s]",
                getClass().getSimpleName(), id, timestamp, channels);
    }
}
