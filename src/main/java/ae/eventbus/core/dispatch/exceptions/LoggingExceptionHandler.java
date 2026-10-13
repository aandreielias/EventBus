package ae.eventbus.core.dispatch.exceptions;

import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Default exception handler that logs subscriber failures via Java Platform Logging
 * without rethrowing, maintaining event delivery continuity and zero runtime dependencies.
 */
public final class LoggingExceptionHandler implements SubscriberExceptionHandler {

    private static final System.Logger LOGGER = System.getLogger(LoggingExceptionHandler.class.getName());

    @Override
    public void handleException(Throwable throwable, Subscriber<?> subscriber, Event event) {
        LOGGER.log(System.Logger.Level.ERROR, String.format(
                "Exception delivering event [id=%s] to subscriber [id=%s, channel=%s]: %s",
                event.getId(), subscriber.getId(), subscriber.getChannel(), throwable.getMessage()
        ), throwable);
    }
}
