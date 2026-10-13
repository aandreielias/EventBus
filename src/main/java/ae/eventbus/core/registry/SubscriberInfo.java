package ae.eventbus.core.registry;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.subscription.DispatchMode;
import ae.eventbus.core.subscription.Priority;
import ae.eventbus.core.subscription.ReferenceType;

/**
 * Immutable audit DTO capturing subscriber metadata and complete lifecycle timestamps.
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
        Instant unsubscribedAt
) {
    public Optional<Instant> getUnsubscribedAt() {
        return Optional.ofNullable(unsubscribedAt);
    }
}