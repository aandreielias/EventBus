package ae.eventbus.core.subscription;

/**
 * Declares the execution threading model for a subscriber.
 */
public enum DispatchMode {

    /**
     * The subscriber is executed immediately on the publisher's calling thread.
     * Fastest execution, but long-running tasks can block the publisher.
     */
    SYNCHRONOUS,

    /**
     * The subscriber is executed asynchronously via a background worker/thread pool.
     * Prevents blocking the publisher thread.
     */
    ASYNCHRONOUS
}