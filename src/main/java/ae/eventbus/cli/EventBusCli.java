package ae.eventbus.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.List;

/**
 * Terminal client for inspecting and controlling EventBus instances exposed with
 * {@link ae.eventbus.core.admin.EventBusAdmin}.
 *
 * <pre>
 * ebus --pid 4242 channels          one-shot command
 * ebus --pid 4242                   interactive shell
 * ebus --host db01:9010 --user ops  remote JMX with authentication
 * </pre>
 *
 * <p>Exit codes: {@code 0} success, {@code 1} connection, permission or remote failure,
 * {@code 2} usage error or invalid target (unknown bus, process not attachable).</p>
 */
public final class EventBusCli {

    static final int OK = 0;
    static final int FAILURE = 1;
    static final int USAGE_ERROR = 2;

    private EventBusCli() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.in, System.out, System.err));
    }

    /** Runs the CLI against the given streams and returns the exit code. */
    static int run(String[] args, InputStream input, PrintStream out, PrintStream err) {
        CliArguments arguments;
        try {
            arguments = CliArguments.parse(args);
        } catch (CliException e) {
            err.println("error: " + e.getMessage());
            err.println(CliArguments.USAGE);
            return USAGE_ERROR;
        }
        if (arguments.help()) {
            out.println(CliArguments.USAGE);
            return OK;
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(input));
        try (AdminClient client = AdminClient.connect(arguments)) {
            Commands commands = new Commands(client, arguments.busName(), out, in);
            if (!arguments.command().isEmpty()) {
                commands.execute(arguments.command());
                return OK;
            }
            return shell(client, commands, in, out, err);
        } catch (Exception e) {
            return report(e, err);
        }
    }

    /** Interactive read-eval-print loop. Errors are reported and the shell keeps running. */
    static int shell(AdminClient client, Commands commands, BufferedReader in, PrintStream out, PrintStream err)
            throws IOException {
        out.println("Connected. Buses: " + client.busNames() + ". Type 'help' for commands, 'exit' to quit.");
        while (true) {
            out.print(commands.busName() == null ? "ebus> " : "ebus:" + commands.busName() + "> ");
            out.flush();
            String line = in.readLine();
            if (line == null) {
                out.println();
                return OK;
            }
            List<String> tokens = Arrays.stream(line.trim().split("\\s+")).filter(t -> !t.isEmpty()).toList();
            if (tokens.isEmpty()) {
                continue;
            }
            if (tokens.get(0).equals("exit") || tokens.get(0).equals("quit")) {
                return OK;
            }
            try {
                commands.execute(tokens);
            } catch (Exception e) {
                report(e, err);
            }
        }
    }

    /** Prints a one-line description of the failure and maps it to an exit code. */
    private static int report(Exception e, PrintStream err) {
        Throwable cause = e instanceof UndeclaredThrowableException u && u.getCause() != null ? u.getCause() : e;
        if (cause instanceof CliException) {
            err.println("error: " + cause.getMessage());
            return USAGE_ERROR;
        }
        if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            err.println("interrupted");
        } else if (cause instanceof SecurityException) {
            err.println("denied: " + cause.getMessage());
        } else if (cause instanceof IOException) {
            err.println("connection failed: " + cause.getMessage());
        } else {
            err.println("error: " + cause.getClass().getSimpleName() + ": " + cause.getMessage());
        }
        return FAILURE;
    }
}
