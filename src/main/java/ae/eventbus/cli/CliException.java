package ae.eventbus.cli;

/**
 * A user-facing error: bad arguments, unknown bus, missing target. Printed without a stack trace.
 */
final class CliException extends RuntimeException {

    CliException(String message) {
        super(message);
    }

    CliException(String message, Throwable cause) {
        super(message, cause);
    }
}
