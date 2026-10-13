package ae.eventbus.core;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.dispatch.AsynchronousDispatcher;
import ae.eventbus.core.dispatch.EventDispatcher;
import ae.eventbus.core.dispatch.exceptions.LoggingExceptionHandler;
import ae.eventbus.core.dispatch.exceptions.SubscriberExceptionHandler;
import ae.eventbus.core.dispatch.SynchronousDispatcher;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.registry.DefaultEventRegistry;
import ae.eventbus.core.registry.EventRegistry;
import ae.eventbus.core.registry.NoOpEventRegistry;
import ae.eventbus.core.routing.ExactMatchRouter;
import ae.eventbus.core.routing.EventRouter;
import ae.eventbus.core.routing.HierarchicalRouter;
import ae.eventbus.core.subscription.EventHandler;
import ae.eventbus.core.validation.DefaultEventValidator;
import ae.eventbus.core.validation.EventValidator;
import ae.eventbus.core.validation.NoOpValidator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Fluent builder for creating fully configured {@link EventBus} instances.
 *
 * <p>All settings are optional; sensible defaults are applied automatically:</p>
 * <ul>
 *   <li>Router: {@link HierarchicalRouter}</li>
 *   <li>Dispatcher: {@link SynchronousDispatcher} (with virtual-thread async offload)</li>
 *   <li>Validator: {@link DefaultEventValidator}</li>
 *   <li>Registry: {@link DefaultEventRegistry} (history: 1 000, channels: 10 000)</li>
 *   <li>Separator: {@code "/"}</li>
 *   <li>Default request timeout: 5 seconds</li>
 *   <li>Max concurrent async invocations: {@value EventDispatcher#DEFAULT_MAX_CONCURRENT_ASYNC_TASKS}</li>
 * </ul>
 *
 * <p>A builder may be reused: every {@link #build()} call creates fresh router, dispatcher and
 * registry instances (custom instances passed to {@code withRouter}/{@code withDispatcher} are
 * the caller's responsibility and would be shared).</p>
 *
 * <h2>Incompatible combinations (detected at {@link #build()} time)</h2>
 * <ul>
 *   <li>{@link #withNoOpRegistry()} + {@link #withMaxHistorySize(int)}</li>
 *   <li>{@link #withDispatcher(EventDispatcher)} + {@link #withAsynchronousDispatching()}</li>
 * </ul>
 */
public final class EventBusBuilder {

    private EventRouter customRouter;
    private boolean exactRouting = false;
    private EventDispatcher customDispatcher;
    private SubscriberExceptionHandler exceptionHandler;
    private EventValidator validator;
    private String channelSeparator = Channel.DEFAULT_SEPARATOR;
    private Duration defaultRequestTimeout = Duration.ofSeconds(5);
    private int maxHistorySize = DefaultEventRegistry.DEFAULT_MAX_HISTORY_SIZE;
    private int maxChannels = DefaultEventRegistry.DEFAULT_MAX_CHANNELS;
    private int maxConcurrentAsyncTasks = EventDispatcher.DEFAULT_MAX_CONCURRENT_ASYNC_TASKS;
    private boolean disableRegistry = false;
    private boolean maxHistorySizeExplicitlySet = false;
    private boolean asyncDispatching = false;
    private ExecutorService customExecutor = null;
    private EventHandler<Event> deadLetterHandler = null;
    private final List<Channel> preRegisteredChannels = new ArrayList<>();

    public static EventBusBuilder create() {
        return new EventBusBuilder();
    }

    /** Uses {@link ExactMatchRouter} — O(1) lookup, no parent-channel inheritance. */
    public EventBusBuilder withExactRouting() {
        this.customRouter = null;
        this.exactRouting = true;
        return this;
    }

    /** Uses {@link HierarchicalRouter} — subscribers on parent channels receive child-channel events. */
    public EventBusBuilder withHierarchicalRouting() {
        this.customRouter = null;
        this.exactRouting = false;
        return this;
    }

    /** Supplies a fully custom router implementation. */
    public EventBusBuilder withRouter(EventRouter router) {
        this.customRouter = Objects.requireNonNull(router, "Custom router must not be null.");
        return this;
    }

    /**
     * Supplies a fully custom dispatcher implementation.
     * Incompatible with {@link #withAsynchronousDispatching()} / {@link #withSynchronousDispatching()}.
     */
    public EventBusBuilder withDispatcher(EventDispatcher dispatcher) {
        this.customDispatcher = Objects.requireNonNull(dispatcher, "Dispatcher must not be null.");
        return this;
    }

    /** All subscribers execute on the publisher's calling thread (async subscribers use a virtual-thread pool). */
    public EventBusBuilder withSynchronousDispatching() {
        this.customDispatcher = null;
        this.asyncDispatching = false;
        this.customExecutor = null;
        return this;
    }

    /** All subscribers execute on a virtual-thread pool; the publisher returns immediately. */
    public EventBusBuilder withAsynchronousDispatching() {
        this.customDispatcher = null;
        this.asyncDispatching = true;
        this.customExecutor = null;
        return this;
    }

    /**
     * All subscribers execute on the supplied executor; the publisher returns immediately.
     * The bus takes ownership of the executor and shuts it down on {@link EventBus#close()}.
     */
    public EventBusBuilder withAsynchronousDispatching(ExecutorService executorService) {
        this.customDispatcher = null;
        this.asyncDispatching = true;
        this.customExecutor = Objects.requireNonNull(executorService, "ExecutorService must not be null.");
        return this;
    }

    /** Overrides the default {@link LoggingExceptionHandler} with a custom strategy. */
    public EventBusBuilder withExceptionHandler(SubscriberExceptionHandler exceptionHandler) {
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler, "ExceptionHandler must not be null.");
        return this;
    }

    /** Supplies a fully custom validator. */
    public EventBusBuilder withValidator(EventValidator validator) {
        this.validator = Objects.requireNonNull(validator, "Validator must not be null.");
        return this;
    }

    /** Disables all validation checks. Use only in trusted, high-performance environments. */
    public EventBusBuilder withNoOpValidator() {
        this.validator = new NoOpValidator();
        return this;
    }

    /** Sets the timeout applied to {@code request()} calls that do not specify an explicit timeout. */
    public EventBusBuilder withDefaultRequestTimeout(Duration timeout) {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Timeout must be a positive duration.");
        }
        this.defaultRequestTimeout = timeout;
        return this;
    }

    /** Sets the channel path separator used when parsing string-path arguments (default: {@code "/"}). */
    public EventBusBuilder withChannelSeparator(String separator) {
        if (separator == null || separator.isEmpty()) {
            throw new IllegalArgumentException("Separator cannot be null or empty.");
        }
        this.channelSeparator = separator;
        return this;
    }

    /**
     * Caps how many asynchronous subscriber invocations may run at once (default:
     * {@value EventDispatcher#DEFAULT_MAX_CONCURRENT_ASYNC_TASKS}). When the cap is reached, further
     * invocations run on the publishing thread, which throttles publishers instead of queueing
     * unbounded work. Ignored when a custom dispatcher is supplied.
     */
    public EventBusBuilder withMaxConcurrentAsyncTasks(int maxConcurrentAsyncTasks) {
        if (maxConcurrentAsyncTasks <= 0) {
            throw new IllegalArgumentException("Max concurrent async tasks must be greater than zero.");
        }
        this.maxConcurrentAsyncTasks = maxConcurrentAsyncTasks;
        return this;
    }

    /**
     * Sets the maximum number of event records retained in the history ring buffer.
     * Incompatible with {@link #withNoOpRegistry()}.
     */
    public EventBusBuilder withMaxHistorySize(int maxHistorySize) {
        if (maxHistorySize <= 0) {
            throw new IllegalArgumentException("Max history size must be greater than zero.");
        }
        this.maxHistorySize = maxHistorySize;
        this.maxHistorySizeExplicitlySet = true;
        return this;
    }

    /** Sets the maximum number of distinct channels the registry will track (default: 10 000). */
    public EventBusBuilder withMaxChannels(int maxChannels) {
        if (maxChannels <= 0) {
            throw new IllegalArgumentException("Max channels must be greater than zero.");
        }
        this.maxChannels = maxChannels;
        return this;
    }

    /** Replaces the registry with a no-op implementation for maximum throughput. Disables all introspection. */
    public EventBusBuilder withNoOpRegistry() {
        this.disableRegistry = true;
        return this;
    }

    /**
     * Registers a dead-letter handler invoked whenever an event is published to a channel
     * with no matching subscribers. Exceptions thrown by the handler are logged and never
     * propagate to the publisher.
     */
    public EventBusBuilder withDeadLetterHandler(EventHandler<Event> handler) {
        this.deadLetterHandler = Objects.requireNonNull(handler, "Dead-letter handler must not be null.");
        return this;
    }

    /** Pre-registers channels in the registry before the bus is opened (useful for dashboards). */
    public EventBusBuilder preRegisterChannels(Channel... channels) {
        if (channels != null) {
            for (Channel ch : channels) {
                if (ch != null) preRegisteredChannels.add(ch);
            }
        }
        return this;
    }

    /**
     * Builds and returns a fully configured {@link EventBus} instance.
     *
     * @throws IllegalStateException if incompatible settings are detected
     */
    public EventBus build() {
        if (disableRegistry && maxHistorySizeExplicitlySet) {
            throw new IllegalStateException(
                    "withNoOpRegistry() and withMaxHistorySize() are incompatible — remove one of them.");
        }
        if (customDispatcher != null && asyncDispatching) {
            throw new IllegalStateException(
                    "withDispatcher(EventDispatcher) and withAsynchronousDispatching() are incompatible — " +
                    "use withDispatcher() alone, or remove it and use withAsynchronousDispatching().");
        }

        EventRouter router = customRouter != null ? customRouter
                : exactRouting ? new ExactMatchRouter() : new HierarchicalRouter();
        SubscriberExceptionHandler handler = exceptionHandler != null ? exceptionHandler : new LoggingExceptionHandler();

        EventDispatcher dispatcher = customDispatcher;
        if (dispatcher == null) {
            if (asyncDispatching) {
                ExecutorService executor = customExecutor != null ? customExecutor : Executors.newVirtualThreadPerTaskExecutor();
                dispatcher = new AsynchronousDispatcher(executor, handler, maxConcurrentAsyncTasks);
            } else {
                dispatcher = new SynchronousDispatcher(handler, Executors.newVirtualThreadPerTaskExecutor(), maxConcurrentAsyncTasks);
            }
        }

        EventValidator effectiveValidator = validator != null ? validator
                : new DefaultEventValidator(DefaultEventValidator.DEFAULT_MAX_CHANNEL_DEPTH, channelSeparator);
        EventRegistry registry = disableRegistry
                ? new NoOpEventRegistry()
                : new DefaultEventRegistry(router, maxHistorySize, maxChannels);
        for (Channel ch : preRegisteredChannels) {
            registry.registerChannel(ch);
        }

        return new DefaultEventBus(router, dispatcher, registry, effectiveValidator, channelSeparator,
                defaultRequestTimeout, deadLetterHandler);
    }
}
