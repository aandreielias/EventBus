package ae.eventbus.core.subscription;

import java.lang.ref.WeakReference;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.event.EventEnvelope;
import ae.eventbus.core.request.RequestEnvelope;

/**
 * Encapsulates an active subscriber: its routing rules, handler, lifecycle timestamps
 * and delivery statistics.
 *
 * <h2>Reference semantics</h2>
 * <p>The handler itself is always held strongly. With {@link ReferenceType#WEAK} the bus
 * holds a weak reference to a separate <em>subscriber target</em> (typically the object that
 * owns the handler) and deregisters the subscription automatically once that target has been
 * garbage-collected. A {@code WEAK} subscription without a target is rejected, because a
 * weakly referenced lambda would be collected almost immediately and silently stop receiving
 * events.</p>
 *
 * @param <T> the type of event payload this subscriber listens to
 */
public final class Subscriber<T> implements Comparable<Subscriber<?>> {

    private static final AtomicLong REGISTRATION_ORDER = new AtomicLong();

    private final UUID id;
    private final long registrationOrder = REGISTRATION_ORDER.incrementAndGet();
    private final Channel channel;
    private final Class<T> eventType;
    private final Priority priority;
    private final DispatchMode dispatchMode;
    private final ReferenceType referenceType;
    private final boolean handlesChannelChildren;

    private final Instant subscribedAt;
    private volatile Instant unsubscribedAt;

    private final EventHandler<T> handler;
    private final WeakReference<Object> weakTarget;
    private final Predicate<Event> eventFilter;
    private final DefaultSubscription subscription;

    private final LongAdder deliveredCount = new LongAdder();
    private final LongAdder failedCount = new LongAdder();

    /**
     * Creates a subscriber without an event filter or subscriber target.
     *
     * @throws IllegalArgumentException if {@code referenceType} is {@link ReferenceType#WEAK}
     */
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

    /**
     * Creates a fully configured subscriber.
     *
     * @param channel                the channel this subscriber is bound to
     * @param eventType              payload (or event) type this subscriber accepts
     * @param priority               invocation priority
     * @param dispatchMode           threading model
     * @param referenceType          {@link ReferenceType#WEAK} ties the subscription's lifetime to {@code subscriberTarget}
     * @param handlesChannelChildren whether events published on descendant channels are delivered too
     * @param handler                the handler invoked for matching events
     * @param onUnsubscribe          optional callback run exactly once when the subscription ends
     * @param eventFilter            optional additional predicate an event must satisfy
     * @param subscriberTarget       object whose reachability controls a {@code WEAK} subscription; ignored for {@code STRONG}
     * @throws IllegalArgumentException if {@code referenceType} is {@code WEAK} and {@code subscriberTarget} is {@code null}
     */
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
        this.handler = Objects.requireNonNull(handler, "Handler must not be null.");
        this.handlesChannelChildren = handlesChannelChildren;
        this.subscribedAt = Instant.now();
        this.eventFilter = eventFilter;

        if (referenceType == ReferenceType.WEAK) {
            if (subscriberTarget == null) {
                throw new IllegalArgumentException(
                        "WEAK subscriptions require a subscriber target; a weakly held handler would be " +
                        "garbage-collected immediately. Use subscribeWeak(...) or ReferenceType.STRONG.");
            }
            this.weakTarget = new WeakReference<>(subscriberTarget);
        } else {
            this.weakTarget = null;
        }

        this.subscription = new DefaultSubscription(this.id, channel, priority, dispatchMode, () -> {
            this.unsubscribedAt = Instant.now();
            if (onUnsubscribe != null) {
                onUnsubscribe.run();
            }
        });
    }

    /** Returns the handler invoked for matching events. */
    public EventHandler<T> getHandler() {
        return handler;
    }

    /**
     * Returns {@code true} while the subscription is active and, for {@code WEAK} subscriptions,
     * its target is still reachable. A collected target ends the subscription as a side effect.
     */
    public boolean isValid() {
        if (!subscription.isActive()) {
            return false;
        }
        if (weakTarget != null && weakTarget.get() == null) {
            subscription.unsubscribe();
            return false;
        }
        return true;
    }

    /** Returns {@code true} if this subscriber is valid and accepts the given event. */
    public boolean canHandle(Event event) {
        if (!isValid() || event == null) {
            return false;
        }
        if (eventFilter != null && !eventFilter.test(event)) {
            return false;
        }
        if (event instanceof RequestEnvelope<?, ?> req) {
            if (!RequestEnvelope.class.isAssignableFrom(eventType)) {
                return false;
            }
            return req.getTargetSubscriberId().map(this.id::equals).orElse(true);
        }
        if (event instanceof EventEnvelope<?> envelope) {
            return eventType.isInstance(envelope.getPayload());
        }
        return eventType.isInstance(event);
    }

    /**
     * Delivers the event to the handler if this subscriber accepts it, updating the
     * delivered/failed counters.
     *
     * @return {@code true} if the handler was invoked successfully, {@code false} if the event was not accepted
     * @throws Exception whatever the handler throws
     */
    public boolean invoke(Event event) throws Exception {
        if (!canHandle(event)) {
            return false;
        }
        Object payload = (event instanceof EventEnvelope<?> envelope) ? envelope.getPayload() : event;
        boolean succeeded = false;
        try {
            handler.handle(eventType.cast(payload));
            succeeded = true;
            return true;
        } finally {
            (succeeded ? deliveredCount : failedCount).increment();
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

    /** Number of events the handler processed without throwing. */
    public long getDeliveredCount() { return deliveredCount.sum(); }

    /** Number of events for which the handler threw. */
    public long getFailedCount() { return failedCount.sum(); }

    /** Orders by priority (highest first), then by creation order, so equal priorities run first-come first-served. */
    @Override
    public int compareTo(Subscriber<?> other) {
        if (other == null) return 1;
        int priorityCompare = Integer.compare(other.priority.getValue(), this.priority.getValue());
        if (priorityCompare != 0) {
            return priorityCompare;
        }
        return Long.compare(this.registrationOrder, other.registrationOrder);
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
