package ae.eventbus.core.dispatch;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ae.eventbus.core.dispatch.exceptions.LoggingExceptionHandler;
import ae.eventbus.core.dispatch.exceptions.SubscriberExceptionHandler;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Dispatches all subscribers asynchronously via an {@link ExecutorService}.
 *
 * <p>The publishing thread normally returns immediately. When {@code maxConcurrentTasks}
 * invocations are already running, further invocations run on the publishing thread
 * (backpressure). See {@link EventDispatcher#DEFAULT_MAX_CONCURRENT_ASYNC_TASKS}.</p>
 */
public final class AsynchronousDispatcher implements EventDispatcher {

    private final SubscriberInvoker invoker;

    /**
     * Creates a dispatcher backed by the supplied executor.
     *
     * @param executorService    runs subscribers; shut down by {@link #close()}
     * @param exceptionHandler   receives subscriber failures
     * @param maxConcurrentTasks upper bound on concurrently running invocations
     */
    public AsynchronousDispatcher(ExecutorService executorService, SubscriberExceptionHandler exceptionHandler, int maxConcurrentTasks) {
        this.invoker = new SubscriberInvoker(exceptionHandler, executorService, maxConcurrentTasks);
    }

    /** Creates a dispatcher bounded by {@link EventDispatcher#DEFAULT_MAX_CONCURRENT_ASYNC_TASKS}. */
    public AsynchronousDispatcher(ExecutorService executorService, SubscriberExceptionHandler exceptionHandler) {
        this(executorService, exceptionHandler, DEFAULT_MAX_CONCURRENT_ASYNC_TASKS);
    }

    /** Creates a dispatcher with virtual threads and the supplied exception handler. */
    public AsynchronousDispatcher(SubscriberExceptionHandler exceptionHandler) {
        this(Executors.newVirtualThreadPerTaskExecutor(), exceptionHandler);
    }

    /** Creates a dispatcher with virtual threads and the default logging handler. */
    public AsynchronousDispatcher() {
        this(new LoggingExceptionHandler());
    }

    @Override
    public void dispatch(Event event, List<Subscriber<?>> subscribers) {
        if (subscribers != null) {
            invoker.invokeAll(event, subscribers, true);
        }
    }

    /** Graceful shutdown: waits up to 5 seconds for in-flight tasks before forcing termination. */
    @Override
    public void close() {
        invoker.close();
    }
}
