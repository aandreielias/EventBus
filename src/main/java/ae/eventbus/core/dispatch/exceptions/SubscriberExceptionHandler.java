package ae.eventbus.core.dispatch.exceptions;

import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Strategy interface invoked when an unhandled exception occurs during subscriber execution.
 * Prevents single subscriber failures from crashing the entire dispatch pipeline.
 */
@FunctionalInterface
public interface SubscriberExceptionHandler {

    /**
     * Handles an exception thrown by a subscriber handler.
     *
     * @param throwable  the unhandled exception
     * @param subscriber the failing subscriber
     * @param event      the event being dispatched
     */
    void handleException(Throwable throwable, Subscriber<?> subscriber, Event event);
}
