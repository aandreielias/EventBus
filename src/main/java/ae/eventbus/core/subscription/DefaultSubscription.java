package ae.eventbus.core.subscription;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import ae.eventbus.core.channel.Channel;

/**
 * Thread-safe subscription token.
 * The active flag is set to {@code false} via a CAS before the deregister action runs,
 * so the subscription is always considered inactive even if the action throws.
 */
public final class DefaultSubscription implements Subscription {

    private final UUID id;
    private final Channel channel;
    private final Priority priority;
    private final DispatchMode dispatchMode;
    private final Runnable unsubscribeAction;
    private final AtomicBoolean active = new AtomicBoolean(true);

    public DefaultSubscription(UUID id,
                               Channel channel,
                               Priority priority,
                               DispatchMode dispatchMode,
                               Runnable unsubscribeAction) {
        this.id = Objects.requireNonNull(id, "ID must not be null.");
        this.channel = Objects.requireNonNull(channel, "Channel must not be null.");
        this.priority = Objects.requireNonNull(priority, "Priority must not be null.");
        this.dispatchMode = Objects.requireNonNull(dispatchMode, "DispatchMode must not be null.");
        this.unsubscribeAction = Objects.requireNonNull(unsubscribeAction, "Unsubscribe action must not be null.");
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public Priority getPriority() {
        return priority;
    }

    @Override
    public DispatchMode getDispatchMode() {
        return dispatchMode;
    }

    @Override
    public boolean isActive() {
        return active.get();
    }

    @Override
    public void unsubscribe() {
        if (active.compareAndSet(true, false)) {
            try {
                unsubscribeAction.run();
            } catch (RuntimeException e) {
                throw e;
            } catch (Throwable t) {
                throw new RuntimeException("Exception during unsubscribe action", t);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Subscription other)) return false;
        return Objects.equals(id, other.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return String.format("Subscription[id=%s, channel=%s, priority=%s, mode=%s, active=%s]",
                id, channel, priority, dispatchMode, isActive());
    }
}
