package ae.eventbus.core.subscription;

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
     * Uses {@link java.lang.ref.WeakReference}. Allows the garbage collector to reclaim
     * the subscriber target if no other strong references exist.
     */
    WEAK
}