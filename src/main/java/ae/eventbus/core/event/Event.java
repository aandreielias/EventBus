package ae.eventbus.core.event;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import ae.eventbus.core.channel.Channel;

/**
 * Core contract for every event flowing through the bus.
 * Each event is uniquely identified, timestamped at creation, and bound to one or more channels.
 */
public interface Event {

    /** Returns the globally unique identifier for this event. */
    UUID getId();

    /** Returns the instant at which this event was created. */
    Instant getTimestamp();

    /**
     * Returns the set of channels this event is addressed to.
     * The bus delivers the event to subscribers on any of these channels.
     *
     * @return unmodifiable, non-empty set of channels
     */
    Set<Channel> getChannels();

    /**
     * Returns the primary channel for this event.
     *
     * <p><strong>Default implementation note:</strong> calls {@code getChannels().iterator().next()},
     * which is non-deterministic when the backing set has no defined iteration order (e.g. {@link java.util.HashSet}).
     * Extend {@link AbstractEvent} or override this method to guarantee a stable primary channel.</p>
     *
     * @return a channel from {@link #getChannels()} — deterministic only for {@link AbstractEvent} subclasses
     */
    default Channel getPrimaryChannel() {
        return getChannels().iterator().next();
    }
}
