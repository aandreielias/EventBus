package ae.eventbus.core.dispatch;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import ae.eventbus.core.dispatch.exceptions.SubscriberExceptionHandler;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.DispatchMode;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Invocation machinery shared by the built-in dispatchers.
 *
 * <ul>
 *   <li>Subscriber failures go to the {@link SubscriberExceptionHandler}; if that handler
 *       itself throws, the failure is logged and delivery to the remaining subscribers continues.</li>
 *   <li>Asynchronous invocations are bounded by {@code maxConcurrentTasks}. When every permit is
 *       taken the task runs on the publishing thread instead (caller-runs backpressure): the
 *       publisher slows down rather than queueing unbounded work, and nested publications from
 *       inside async handlers cannot deadlock waiting for permits.</li>
 * </ul>
 */
final class SubscriberInvoker implements AutoCloseable {

    static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

    private static final System.Logger LOGGER = System.getLogger(SubscriberInvoker.class.getName());

    private final SubscriberExceptionHandler exceptionHandler;
    private final ExecutorService executor;
    private final Semaphore permits;

    SubscriberInvoker(SubscriberExceptionHandler exceptionHandler, ExecutorService executor, int maxConcurrentTasks) {
        if (maxConcurrentTasks <= 0) {
            throw new IllegalArgumentException("Max concurrent async tasks must be greater than zero.");
        }
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler, "ExceptionHandler must not be null.");
        this.executor = Objects.requireNonNull(executor, "ExecutorService must not be null.");
        this.permits = new Semaphore(maxConcurrentTasks);
    }

    /** Invokes the subscriber on the current thread, never throwing. */
    void invokeNow(Subscriber<?> subscriber, Event event) {
        try {
            subscriber.invoke(event);
        } catch (Throwable failure) {
            try {
                exceptionHandler.handleException(failure, subscriber, event);
            } catch (Throwable handlerFailure) {
                handlerFailure.addSuppressed(failure);
                LOGGER.log(System.Logger.Level.ERROR,
                        "SubscriberExceptionHandler threw while handling a failure of subscriber " + subscriber.getId(),
                        handlerFailure);
            }
        }
    }

    /** Invokes the subscriber on the executor, or on the current thread when the concurrency limit is reached. */
    void invokeAsync(Subscriber<?> subscriber, Event event) {
        if (!permits.tryAcquire()) {
            invokeNow(subscriber, event);
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    invokeNow(subscriber, event);
                } finally {
                    permits.release();
                }
            });
        } catch (RejectedExecutionException e) {
            permits.release();
            throw e;
        }
    }

    /** Dispatches each subscriber according to its own {@link DispatchMode}. */
    void invokeAll(Event event, List<Subscriber<?>> subscribers, boolean forceAsync) {
        for (Subscriber<?> subscriber : subscribers) {
            if (forceAsync || subscriber.getDispatchMode() == DispatchMode.ASYNCHRONOUS) {
                invokeAsync(subscriber, event);
            } else {
                invokeNow(subscriber, event);
            }
        }
    }

    /** Graceful shutdown: waits up to {@value #SHUTDOWN_TIMEOUT_SECONDS} seconds for in-flight tasks, then interrupts them. */
    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
