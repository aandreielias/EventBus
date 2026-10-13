package ae.eventbus.core.subscription;

import java.lang.ref.WeakReference;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.event.EventEnvelope;

/**
 * Encapsulates an active subscriber, its execution rules, metadata, handler invocation,
 * and lifecycle timestamps.
 *
 * @param <T> the type of event payload this subscriber listens to
 */
public final class Subscriber<T> implements Comparable<Subscriber<?>> {

    private final UUID id;
    private final Channel channel;
    private final Class<T> eventType;
    private final Priority priority;
    private final DispatchMode dispatchMode;
    private final ReferenceType referenceType;
    private final boolean handlesChannelChildren;

    private final Instant subscribedAt;
    private volatile Instant unsubscribedAt;

    private final EventHandler<T> strongHandler;
    private final WeakReference<EventHandler<T>> weakHandler;
    private final WeakReference<Object> weakTarget;
    private final Predicate<Event> eventFilter;
    private final DefaultSubscription subscription;

    public Subscriber(Channel channel,
                      Class<T> eventType,
                      Priority priority,
                      DispatchMode dispatchMode,
                      ReferenceType referenceType,
                      boolean handlesChannelChildren,
                      EventHandler<T> handler,
                      Runnable onUnsubscribe) {
        this(channel, eventType, priority, dispatchMode, referenceType, handlesChannelChildren, handler, onUnsubscribe, null, null);
    }

    public Subscriber(Channel channel,
                      Class<T> eventType,
                      Priority priority,
                      DispatchMode dispatchMode,
                      ReferenceType referenceType,
                      boolean handlesChannelChildren,
                      EventHandler<T> handler,
                      Runnable onUnsubscribe,
                      Predicate<Event> eventFilter,
                      Object subscriberTarget) {
        this.id = UUID.randomUUID();
        this.channel = Objects.requireNonNull(channel, "Channel must not be null.");
        this.eventType = Objects.requireNonNull(eventType, "EventType must not be null.");
        this.priority = Objects.requireNonNull(priority, "Priority must not be null.");
        this.dispatchMode = Objects.requireNonNull(dispatchMode, "DispatchMode must not be null.");
        this.referenceType = Objects.requireNonNull(referenceType, "ReferenceType must not be null.");
        this.handlesChannelChildren = handlesChannelChildren;
        this.subscribedAt = Instant.now();
        this.eventFilter = eventFilter;

        Objects.requireNonNull(handler, "Handler must not be null.");

        if (subscriberTarget != null) {
            this.weakTarget = new WeakReference<>(subscriberTarget);
            this.strongHandler = handler;
            this.weakHandler = null;
        } else if (referenceType == ReferenceType.STRONG) {
            this.weakTarget = null;
            this.strongHandler = handler;
            this.weakHandler = null;
        } else {
            this.weakTarget = null;
            this.strongHandler = null;
            this.weakHandler = new WeakReference<>(handler);
        }

        // Hook unsubscription to capture timestamp
        this.subscription = new DefaultSubscription(this.id, channel, priority, dispatchMode, () -> {
            this.unsubscribedAt = Instant.now();
            if (onUnsubscribe != null) {
                onUnsubscribe.run();
            }
        });
    }

    public EventHandler<T> getHandler() {
        if (strongHandler != null) {
            return strongHandler;
        }
        return (weakHandler != null) ? weakHandler.get() : null;
    }

    public boolean isValid() {
        if (!subscription.isActive()) {
            return false;
        }
        if (weakTarget != null && weakTarget.get() == null) {
            subscription.unsubscribe();
            return false;
        }
        if (referenceType == ReferenceType.WEAK && weakHandler != null && weakHandler.get() == null) {
            subscription.unsubscribe();
            return false;
        }
        return true;
    }

    public boolean canHandle(Event event) {
        if (!isValid() || event == null) {
            return false;
        }
        if (eventFilter != null && !eventFilter.test(event)) {
            return false;
        }
        if (event instanceof ae.eventbus.core.request.RequestEnvelope<?, ?> req) {
            if (!ae.eventbus.core.request.RequestEnvelope.class.isAssignableFrom(eventType)) {
                return false;
            }
            if (req.getTargetSubscriberId().isPresent() && !req.getTargetSubscriberId().get().equals(this.id)) {
                return false;
            }
            return true;
        }
        if (event instanceof EventEnvelope<?> envelope) {
            return eventType.isInstance(envelope.getPayload());
        }
        return eventType.isInstance(event);
    }

    @SuppressWarnings("unchecked")
    public void invoke(Event event) throws Exception {
        if (!canHandle(event)) {
            return;
        }

        EventHandler<T> handler = getHandler();
        if (handler == null) {
            return;
        }

        if (event instanceof EventEnvelope<?> envelope) {
            if (eventType.isInstance(envelope.getPayload())) {
                handler.handle((T) envelope.getPayload());
            }
        } else if (eventType.isInstance(event)) {
            handler.handle((T) event);
        }
    }

    public UUID getId() { return id; }
    public Channel getChannel() { return channel; }
    public Class<T> getEventType() { return eventType; }
    public Priority getPriority() { return priority; }
    public DispatchMode getDispatchMode() { return dispatchMode; }
    public ReferenceType getReferenceType() { return referenceType; }
    public boolean isHandlesChannelChildren() { return handlesChannelChildren; }
    public Subscription getSubscription() { return subscription; }
    public Instant getSubscribedAt() { return subscribedAt; }
    public Instant getUnsubscribedAt() { return unsubscribedAt; }

    @Override
    public int compareTo(Subscriber<?> other) {
        if (other == null) return 1;
        int priorityCompare = Integer.compare(other.priority.getValue(), this.priority.getValue());
        if (priorityCompare != 0) {
            return priorityCompare;
        }
        return this.id.compareTo(other.id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Subscriber<?> other)) return false;
        return Objects.equals(this.id, other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}