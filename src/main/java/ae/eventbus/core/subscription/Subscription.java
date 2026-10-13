package ae.eventbus.core.subscription;

import java.util.UUID;

import ae.eventbus.core.channel.Channel;

/**
 * Represents an active subscription to a channel or event.
 * <p>
 * Implements {@link AutoCloseable} so that subscriptions can be managed
 * conveniently within try-with-resources blocks or component lifecycles.
 * Calling {@link #unsubscribe()} or {@link #close()} removes the subscriber
 * and prevents memory leaks.
 * </p>
 */
public interface Subscription extends AutoCloseable {

    /**
     * Unique identifier for this specific subscription instance.
     *
     * @return subscription {@link UUID}
     */
    UUID getId();

    /**
     * Returns the target channel to which this subscription is bound.
     *
     * @return target {@link Channel}
     */
    Channel getChannel();

    /**
     * Returns the priority of this subscriber.
     *
     * @return {@link Priority}
     */
    Priority getPriority();

    /**
     * Returns whether this subscriber expects synchronous or asynchronous execution.
     *
     * @return {@link DispatchMode}
     */
    DispatchMode getDispatchMode();

    /**
     * Checks if this subscription is still actively registered and listening.
     *
     * @return {@code true} if active, {@code false} if unsubscribed
     */
    boolean isActive();

    /**
     * Cancels this subscription and deregisters the subscriber from the event bus.
     * Subsequent event dispatches will no longer invoke this subscriber.
     */
    void unsubscribe();

    /**
     * Alias for {@link #unsubscribe()} to fulfill the {@link AutoCloseable} contract.
     */
    @Override
    default void close() {
        unsubscribe();
    }
    
}
