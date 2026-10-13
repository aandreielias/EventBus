package ae.eventbus.core.dispatch;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ae.eventbus.core.dispatch.exceptions.LoggingExceptionHandler;
import ae.eventbus.core.dispatch.exceptions.SubscriberExceptionHandler;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.DispatchMode;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Dispatches synchronous subscribers on the caller's thread and offloads
 * {@link DispatchMode#ASYNCHRONOUS} subscribers to an executor (virtual threads by default).
 *
 * <p>At most {@code maxConcurrentAsyncTasks} async invocations run at once; beyond that they run
 * on the publishing thread. See {@link EventDispatcher#DEFAULT_MAX_CONCURRENT_ASYNC_TASKS}.</p>
 */
public final class SynchronousDispatcher implements EventDispatcher {

    private final SubscriberInvoker invoker;

    /**
     * Creates a dispatcher whose async subscribers run on the supplied executor.
     *
     * @param exceptionHandler        receives subscriber failures
     * @param asyncExecutor           runs async subscribers; shut down by {@link #close()}
     * @param maxConcurrentAsyncTasks upper bound on concurrently running async invocations
     */
    public SynchronousDispatcher(SubscriberExceptionHandler exceptionHandler, ExecutorService asyncExecutor, int maxConcurrentAsyncTasks) {
        this.invoker = new SubscriberInvoker(exceptionHandler, asyncExecutor, maxConcurrentAsyncTasks);
    }

    /** Creates a dispatcher bounded by {@link EventDispatcher#DEFAULT_MAX_CONCURRENT_ASYNC_TASKS}. */
    public SynchronousDispatcher(SubscriberExceptionHandler exceptionHandler, ExecutorService asyncExecutor) {
        this(exceptionHandler, asyncExecutor, DEFAULT_MAX_CONCURRENT_ASYNC_TASKS);
    }

    /** Creates a dispatcher using virtual threads for async subscribers. */
    public SynchronousDispatcher(SubscriberExceptionHandler exceptionHandler) {
        this(exceptionHandler, Executors.newVirtualThreadPerTaskExecutor());
    }

    /** Creates a dispatcher using virtual threads and the default logging exception handler. */
    public SynchronousDispatcher() {
        this(new LoggingExceptionHandler());
    }

    @Override
    public void dispatch(Event event, List<Subscriber<?>> subscribers) {
        if (subscribers != null) {
            invoker.invokeAll(event, subscribers, false);
        }
    }

    @Override
    public void close() {
        invoker.close();
    }
}
