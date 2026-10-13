package ae.eventbus.cli;

import java.util.List;

/**
 * Parsed global command-line options. Options are read until the first non-option token;
 * that token and everything after it form the command.
 *
 * @param pid     local JVM to attach to, or {@code null}
 * @param jmxUrl  remote JMX service URL, or {@code null}
 * @param busName bus to operate on; {@code null} selects the only registered bus
 * @param user    JMX user for remote connections, or {@code null}
 * @param help    whether usage help was requested
 * @param command command name and its arguments; empty starts the interactive shell
 */
record CliArguments(Long pid, String jmxUrl, String busName, String user, boolean help, List<String> command) {

    static final String USAGE = """
            Usage: ebus (--pid <pid> | --host <host:port> | --url <jmx-url>) [--bus <name>] [--user <name>] [command [args]]

            Connects to a JVM running an EventBus registered with EventBusAdmin.
            Without a command, starts an interactive shell.

            Options:
              --pid <pid>         attach to a local JVM (same OS user, no network port needed)
              --host <host:port>  connect to a remote JMX agent
              --url <jmx-url>     connect using a full JMX service URL
              --bus <name>        bus to use when the JVM has several
              --user <name>       JMX user; the password is read from EBUS_PASSWORD or prompted
              -h, --help          show this help

            Run 'ebus --pid <pid> help' for the list of commands.""";

    static CliArguments parse(String[] args) {
        Long pid = null;
        String url = null;
        String bus = null;
        String user = null;
        boolean help = false;

        int i = 0;
        for (; i < args.length && args[i].startsWith("-"); i++) {
            String option = args[i];
            switch (option) {
                case "-h", "--help" -> help = true;
                case "--pid" -> pid = parsePid(value(args, ++i, option));
                case "--host" -> url = "service:jmx:rmi:///jndi/rmi://" + value(args, ++i, option) + "/jmxrmi";
                case "--url" -> url = value(args, ++i, option);
                case "--bus" -> bus = value(args, ++i, option);
                case "--user" -> user = value(args, ++i, option);
                default -> throw new CliException("Unknown option: " + option);
            }
        }
        if (!help && (pid == null) == (url == null)) {
            throw new CliException("Specify exactly one of --pid, --host or --url.");
        }
        return new CliArguments(pid, url, bus, user, help, List.copyOf(List.of(args).subList(i, args.length)));
    }

    private static String value(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new CliException("Option " + option + " requires a value.");
        }
        return args[index];
    }

    private static long parsePid(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new CliException("Not a process id: " + value);
        }
    }
}
