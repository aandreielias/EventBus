package ae.eventbus.core.validation;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;

/**
 * Strategy contract for validating channels, raw payloads, and events
 * prior to publication or subscription within the event bus.
 */
public interface EventValidator {

    /**
     * Validates a channel's structure, depth, or naming conventions.
     *
     * @param channel the channel to validate
     * @throws ValidationException if the channel violates validation rules
     */
    void validateChannel(Channel channel) throws ValidationException;

    /**
     * Validates a standalone {@link Event} instance.
     *
     * @param event the event to validate
     * @throws ValidationException if the event is invalid
     */
    void validateEvent(Event event) throws ValidationException;

    /**
     * Validates an arbitrary payload object before it is wrapped or dispatched.
     *
     * @param channel destination channel
     * @param payload the data payload to validate
     * @param <T>     payload type
     * @throws ValidationException if the payload or channel violates rules
     */
    <T> void validatePayload(Channel channel, T payload) throws ValidationException;
}