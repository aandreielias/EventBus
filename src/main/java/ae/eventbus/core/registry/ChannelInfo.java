package ae.eventbus.core.registry;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import ae.eventbus.core.channel.Channel;

/**
 * Live audit view of a channel's lifecycle and activity statistics.
 *
 * <p>Instances returned by the registry are <strong>live views</strong> — their state
 * changes as events are published. Call {@link #toSnapshot()} for a consistent,
 * point-in-time copy suitable for reporting or serialisation.</p>
 */
public final class ChannelInfo {

    private final Channel channel;
    private final Instant createdAt;
    private Instant lastFiredAt;
    private final AtomicLong publicationCount = new AtomicLong(0);

    public ChannelInfo(Channel channel) {
        this.channel = Objects.requireNonNull(channel, "Channel must not be null.");
        this.createdAt = Instant.now();
    }

    public Channel getChannel() {
        return channel;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    /** Returns the timestamp of the most recent publication, or {@code null} if never fired. */
    public synchronized Instant getLastFiredAt() {
        return lastFiredAt;
    }

    public long getPublicationCount() {
        return publicationCount.get();
    }

    /**
     * Records a publication on this channel.
     * The timestamp and counter are updated atomically under the instance lock
     * so that {@link #toSnapshot()} always sees a consistent pair of values.
     */
    public synchronized void recordFired() {
        this.lastFiredAt = Instant.now();
        this.publicationCount.incrementAndGet();
    }

    /**
     * Returns an immutable point-in-time snapshot of this channel's statistics.
     * Use this when you need a consistent view for logging, metrics export, or serialisation.
     */
    public synchronized ChannelInfoSnapshot toSnapshot() {
        return new ChannelInfoSnapshot(channel, createdAt, lastFiredAt, publicationCount.get());
    }

    @Override
    public String toString() {
        return String.format("ChannelInfo[channel=%s, createdAt=%s, lastFiredAt=%s, publications=%d]",
                channel, createdAt, getLastFiredAt(), publicationCount.get());
    }

    /**
     * Immutable point-in-time snapshot of a channel's audit statistics.
     *
     * @param channel          the channel
     * @param createdAt        when the channel was first registered
     * @param lastFiredAt      timestamp of last publication ({@code null} if never fired)
     * @param publicationCount total number of publications recorded
     */
    public record ChannelInfoSnapshot(
            Channel channel,
            Instant createdAt,
            Instant lastFiredAt,
            long publicationCount
    ) {}
}
