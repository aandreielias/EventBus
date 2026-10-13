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
import ae.eventbus.core.subscription.ReferenceType;
import ae.eventbus.core.validation.DefaultEventValidator;
import ae.eventbus.core.validation.EventValidator;
import ae.eventbus.core.validation.NoOpValidator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

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
 * </ul>
 *
 * <h3>Incompatible combinations (detected at {@link #build()} time):</h3>
 * <ul>
 *   <li>{@link #withNoOpRegistry()} + {@link #withMaxHistorySize(int)}</li>
 *   <li>{@link #withDispatcher(EventDispatcher)} + {@link #withAsynchronousDispatching()}</li>
 * </ul>
 */
public final class EventBusBuilder {

    private EventRouter router;
    private EventDispatcher customDispatcher;
    private SubscriberExceptionHandler exceptionHandler;
    private EventRegistry registry;
    private EventValidator validator;
    private String channelSeparator = Channel.DEFAULT_SEPARATOR;
    private ReferenceType defaultReferenceType = ReferenceType.STRONG;
    private Duration defaultRequestTimeout = Duration.ofSeconds(5);
    private int maxHistorySize = DefaultEventRegistry.DEFAULT_MAX_HISTORY_SIZE;
    private int maxChannels = DefaultEventRegistry.DEFAULT_MAX_CHANNELS;
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
        this.router = new ExactMatchRouter();
        return this;
    }

    /** Uses {@link HierarchicalRouter} — subscribers on parent channels receive child-channel events. */
    public EventBusBuilder withHierarchicalRouting() {
        this.router = new HierarchicalRouter();
        return this;
    }

    /** Supplies a fully custom router implementation. */
    public EventBusBuilder withRouter(EventRouter router) {
        this.router = Objects.requireNonNull(router, "Custom router must not be null.");
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

    /** All subscribers execute on the supplied executor; the publisher returns immediately. */
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

    /** Sets the default reference type for subscriptions that do not specify one (default: {@link ReferenceType#STRONG}). */
    public EventBusBuilder withDefaultReferenceType(ReferenceType referenceType) {
        this.defaultReferenceType = Objects.requireNonNull(referenceType, "ReferenceType must not be null.");
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
     * with no active subscribers. Exceptions thrown by the handler are silently swallowed
     * so they never propagate to the publisher.
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

        if (router == null) router = new HierarchicalRouter();
        if (exceptionHandler == null) exceptionHandler = new LoggingExceptionHandler();

        EventDispatcher dispatcher = this.customDispatcher;
        if (dispatcher == null) {
            if (asyncDispatching) {
                dispatcher = customExecutor != null
                        ? new AsynchronousDispatcher(customExecutor, exceptionHandler)
                        : new AsynchronousDispatcher(exceptionHandler);
            } else {
                dispatcher = new SynchronousDispatcher(exceptionHandler);
            }
        }

        if (validator == null) validator = new DefaultEventValidator();

        if (registry == null) {
            registry = disableRegistry
                    ? new NoOpEventRegistry()
                    : new DefaultEventRegistry(router, maxHistorySize, maxChannels);
        }

        for (Channel ch : preRegisteredChannels) {
            registry.registerChannel(ch);
        }

        return new DefaultEventBus(router, dispatcher, registry, validator, channelSeparator,
                defaultReferenceType, defaultRequestTimeout, deadLetterHandler);
    }
}
