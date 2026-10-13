package ae.eventbus.core.validation;

/**
 * Thrown when an event, payload, or channel fails structural or policy validation
 * before being accepted by the event bus.
 */
public class ValidationException extends RuntimeException {

    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}