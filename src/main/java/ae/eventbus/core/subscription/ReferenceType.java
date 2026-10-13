package ae.eventbus.core.subscription;

import ae.eventbus.core.EventBus;

/**
 * Defines how the event bus references subscriber handlers in memory.
 */
public enum ReferenceType {

    /**
     * Standard strong reference. Prevents the subscriber from being garbage-collected.
     * Requires explicit unsubscription to avoid memory leaks.
     */
    STRONG,

    /**
     * Holds a {@link java.lang.ref.WeakReference} to a separate subscriber target (the object that
     * owns the handler). Once the target is garbage-collected the subscription ends automatically.
     * Requires a target: see {@link EventBus#subscribeWeak}.
     */
    WEAK
}