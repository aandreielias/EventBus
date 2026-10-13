package ae.eventbus.core.subscription;

/**
 * Defines the execution priority of a subscriber.
 * Subscribers with higher priority values are invoked before subscribers with lower priority.
 */
public enum Priority {

    HIGHEST(100),
    HIGH(75),
    NORMAL(50),
    LOW(25),
    MONITOR(0);

    private final int value;

    Priority(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}