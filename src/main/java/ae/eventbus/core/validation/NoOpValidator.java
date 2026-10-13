package ae.eventbus.core.validation;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;

/**
 * High-performance no-op implementation of {@link EventValidator}.
 * Bypasses all validation checks to minimize CPU overhead in trusted environments.
 */
public final class NoOpValidator implements EventValidator {

    @Override
    public void validateChannel(Channel channel) throws ValidationException {
        // No-op
    }

    @Override
    public void validateEvent(Event event) throws ValidationException {
        // No-op
    }

    @Override
    public <T> void validatePayload(Channel channel, T payload) throws ValidationException {
        // No-op
    }
}