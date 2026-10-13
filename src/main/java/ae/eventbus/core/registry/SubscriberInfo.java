package ae.eventbus.core.registry;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.subscription.DispatchMode;
import ae.eventbus.core.subscription.Priority;
import ae.eventbus.core.subscription.ReferenceType;

/**
 * Immutable audit DTO capturing subscriber metadata, lifecycle timestamps and delivery statistics.
 *
 * @param id                     subscription id
 * @param channel                channel the subscriber is registered on
 * @param eventType              accepted payload or event type
 * @param priority               invocation priority
 * @param dispatchMode           threading model
 * @param referenceType          reference semantics
 * @param handlesChannelChildren whether descendant-channel events are delivered too
 * @param active                 whether the subscription was live when the snapshot was taken
 * @param subscribedAt           when the subscription started
 * @param unsubscribedAt         when it ended, or {@code null}
 * @param deliveredCount number of events the handler processed without throwing
 * @param failedCount    number of events for which the handler threw
 */
public record SubscriberInfo(
        UUID id,
        Channel channel,
        Class<?> eventType,
        Priority priority,
        DispatchMode dispatchMode,
        ReferenceType referenceType,
        boolean handlesChannelChildren,
        boolean active,
        Instant subscribedAt,
        Instant unsubscribedAt,
        long deliveredCount,
        long failedCount
) {
    public Optional<Instant> getUnsubscribedAt() {
        return Optional.ofNullable(unsubscribedAt);
    }
}