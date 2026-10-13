package ae.eventbus.core.subscription;

/**
 * Functional interface for handling events dispatched by the event bus.
 *
 * @param <T> the type of event to handle
 */
@FunctionalInterface
public interface EventHandler<T> {

    /**
     * Handles the incoming event.
     *
     * @param event the event to process
     * @throws Exception if an error occurs during handling
     */
    void handle(T event) throws Exception;
}