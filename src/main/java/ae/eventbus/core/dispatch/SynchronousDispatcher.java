package ae.eventbus.core.dispatch;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import ae.eventbus.core.dispatch.exceptions.LoggingExceptionHandler;
import ae.eventbus.core.dispatch.exceptions.SubscriberExceptionHandler;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.DispatchMode;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Dispatches synchronous subscribers on the caller's thread and offloads
 * {@link DispatchMode#ASYNCHRONOUS} subscribers to a virtual-thread executor.
 */
public final class SynchronousDispatcher implements EventDispatcher {

    static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final SubscriberExceptionHandler exceptionHandler;
    private final ExecutorService asyncExecutor;

    public SynchronousDispatcher(SubscriberExceptionHandler exceptionHandler, ExecutorService asyncExecutor) {
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler, "ExceptionHandler must not be null.");
        this.asyncExecutor = Objects.requireNonNull(asyncExecutor, "AsyncExecutor must not be null.");
    }

    public SynchronousDispatcher(SubscriberExceptionHandler exceptionHandler) {
        this(exceptionHandler, Executors.newVirtualThreadPerTaskExecutor());
    }

    public SynchronousDispatcher() {
        this(new LoggingExceptionHandler());
    }

    @Override
    public void dispatch(Event event, List<Subscriber<?>> subscribers) {
        if (subscribers == null || subscribers.isEmpty()) {
            return;
        }

        for (Subscriber<?> subscriber : subscribers) {
            if (subscriber.getDispatchMode() == DispatchMode.ASYNCHRONOUS) {
                asyncExecutor.submit(() -> {
                    try {
                        subscriber.invoke(event);
                    } catch (Throwable t) {
                        exceptionHandler.handleException(t, subscriber, event);
                    }
                });
            } else {
                try {
                    subscriber.invoke(event);
                } catch (Throwable t) {
                    exceptionHandler.handleException(t, subscriber, event);
                }
            }
        }
    }

    @Override
    public void close() {
        asyncExecutor.shutdown();
        try {
            if (!asyncExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                asyncExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            asyncExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
