package ae.eventbus.core.dispatch;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import ae.eventbus.core.dispatch.exceptions.LoggingExceptionHandler;
import ae.eventbus.core.dispatch.exceptions.SubscriberExceptionHandler;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Dispatches all subscribers asynchronously via an {@link ExecutorService}.
 * The publishing thread returns immediately without waiting for subscriber execution.
 */
public final class AsynchronousDispatcher implements EventDispatcher {

    private final ExecutorService executorService;
    private final SubscriberExceptionHandler exceptionHandler;

    /**
     * Creates a dispatcher backed by the supplied executor and exception handler.
     */
    public AsynchronousDispatcher(ExecutorService executorService, SubscriberExceptionHandler exceptionHandler) {
        this.executorService = Objects.requireNonNull(executorService, "ExecutorService must not be null.");
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler, "ExceptionHandler must not be null.");
    }

    /** Creates a dispatcher with virtual threads and the supplied exception handler. */
    public AsynchronousDispatcher(SubscriberExceptionHandler exceptionHandler) {
        this(Executors.newVirtualThreadPerTaskExecutor(), exceptionHandler);
    }

    /** Creates a dispatcher with virtual threads and the default logging handler. */
    public AsynchronousDispatcher() {
        this(Executors.newVirtualThreadPerTaskExecutor(), new LoggingExceptionHandler());
    }

    @Override
    public void dispatch(Event event, List<Subscriber<?>> subscribers) {
        if (subscribers == null || subscribers.isEmpty()) {
            return;
        }

        for (Subscriber<?> subscriber : subscribers) {
            executorService.submit(() -> {
                try {
                    subscriber.invoke(event);
                } catch (Throwable t) {
                    exceptionHandler.handleException(t, subscriber, event);
                }
            });
        }
    }

    /** Graceful shutdown: waits up to 5 seconds for in-flight tasks before forcing termination. */
    @Override
    public void close() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(SynchronousDispatcher.SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
