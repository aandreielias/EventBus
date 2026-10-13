package ae.eventbus.core.admin;

/**
 * What remote administrators may do with a bus exposed through {@link EventBusAdmin}.
 */
public enum AdminAccess {

    /** Inspection only: summary, channels, subscribers and history. The safe default for production. */
    READ_ONLY,

    /** Inspection plus control operations: ending subscriptions and clearing history. */
    READ_WRITE
}
