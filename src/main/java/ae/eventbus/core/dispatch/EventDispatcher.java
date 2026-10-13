package ae.eventbus.core.dispatch;

import java.util.List;

import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Strategy contract defining how resolved subscribers are invoked for an event.
 */
public interface EventDispatcher extends AutoCloseable {

    /**
     * Dispatches an event to the provided list of subscribers.
     *
     * @param event       the event to dispatch
     * @param subscribers the ordered subscribers to invoke
     */
    void dispatch(Event event, List<Subscriber<?>> subscribers);

    /**
     * Closes and cleans up any underlying resources.
     */
    @Override
    default void close() {
    }
}