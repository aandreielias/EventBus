package ae.eventbus.cli;

import ae.eventbus.core.admin.EventBusAdminMXBean;
import ae.eventbus.core.admin.EventBusAdminMXBean.BusSummary;
import ae.eventbus.core.admin.EventBusAdminMXBean.ChannelView;
import ae.eventbus.core.admin.EventBusAdminMXBean.EventView;
import ae.eventbus.core.admin.EventBusAdminMXBean.SubscriberView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The CLI's command set. Each command talks to the selected bus through its MXBean proxy and
 * prints plain-text tables. Used both for one-shot invocations and by the interactive shell.
 */
final class Commands {

    private static final int DEFAULT_HISTORY_LIMIT = 20;
    private static final int TAIL_BATCH_LIMIT = 1_000;
    private static final long DEFAULT_TAIL_INTERVAL_MS = 500;

    private static final DateTimeFormatter DATE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    @FunctionalInterface
    private interface Action {
        void run(List<String> args) throws IOException, InterruptedException;
    }

    private record Command(String usage, String description, Action action) {}

    private record ParsedArgs(Map<String, String> options, List<String> positional) {
        String positionalOrNull() {
            return positional.isEmpty() ? null : positional.get(0);
        }
    }

    private final AdminClient client;
    private final PrintStream out;
    private final BufferedReader in;
    private final Map<String, Command> commands = new LinkedHashMap<>();
    private String busName;

    /**
     * @param client  connection to the target JVM
     * @param busName initially selected bus, or {@code null} for the only registered one
     * @param out     where results are printed
     * @param in      terminal input, used by {@code tail} to detect the stop key
     */
    Commands(AdminClient client, String busName, PrintStream out, BufferedReader in) {
        this.client = client;
        this.busName = busName;
        this.out = out;
        this.in = in;

        define("info", "info", "Show bus summary and configuration", noArgs(args -> info()));
        define("buses", "buses", "List buses registered in the JVM", noArgs(args -> buses()));
        define("use", "use <bus>", "Select the bus for subsequent commands", this::use);
        define("channels", "channels", "List channels with subscriber and publication counts", noArgs(args -> channels()));
        define("subscribers", "subscribers [channel]", "List live subscribers, optionally of one channel", this::subscribers);
        define("history", "history [-n <count>] [channel]", "Show recent publications (channel includes descendants)", this::history);
        define("tail", "tail [--interval <ms>] [channel]", "Follow publications live; press Enter to stop", this::tail);
        define("unsubscribe", "unsubscribe <subscription-id>", "End a subscription (needs READ_WRITE access)", this::unsubscribe);
        define("clear-history", "clear-history", "Clear the publication history (needs READ_WRITE access)", noArgs(args -> clearHistory()));
        define("help", "help", "Show this list", noArgs(args -> help()));
    }

    private void define(String name, String usage, String description, Action action) {
        commands.put(name, new Command(usage, description, action));
    }

    /** Wraps an action so that it rejects any arguments. */
    private static Action noArgs(Action action) {
        return args -> {
            parse(args, Set.of(), 0);
            action.run(args);
        };
    }

    /** Name of the explicitly selected bus, or {@code null} when relying on the single registered bus. */
    String busName() {
        return busName;
    }

    /**
     * Runs one command.
     *
     * @param args command name followed by its arguments
     * @throws CliException for unknown commands or invalid arguments
     */
    void execute(List<String> args) throws IOException, InterruptedException {
        Command command = commands.get(args.get(0));
        if (command == null) {
            throw new CliException("Unknown command '" + args.get(0) + "'. Type 'help' for the list of commands.");
        }
        command.action().run(args.subList(1, args.size()));
    }

    // === COMMANDS ===

    private void info() throws IOException {
        BusSummary s = bus().getSummary();
        out.printf("Bus:            %s%n", s.name());
        out.printf("Access:         %s%n", s.access());
        out.printf("State:          %s%n", s.closed() ? "CLOSED" : "OPEN");
        out.printf("Separator:      %s%n", s.channelSeparator());
        out.printf("Channels:       %d%n", s.channelCount());
        out.printf("Subscribers:    %d%n", s.subscriberCount());
        out.printf("Publications:   %d%n", s.publicationCount());
        out.printf("History:        %d records (last seq %d)%n", s.historySize(), s.lastSequence());
    }

    private void buses() throws IOException {
        TextTable table = new TextTable("BUS", "SELECTED");
        for (String name : client.busNames()) {
            table.row(name, name.equals(busName) ? "*" : "");
        }
        table.print(out);
    }

    private void use(List<String> args) throws IOException {
        String name = parse(args, Set.of(), 1).positionalOrNull();
        if (name == null) {
            throw new CliException("Usage: use <bus>");
        }
        client.bus(name);
        busName = name;
        out.println("Using bus '" + name + "'.");
    }

    private void channels() throws IOException {
        TextTable table = new TextTable("CHANNEL", "SUBSCRIBERS", "PUBLICATIONS", "LAST FIRED");
        for (ChannelView c : bus().getChannels()) {
            table.row(c.channel(), c.subscriberCount(), c.publicationCount(), dateTime(c.lastFiredAt()));
        }
        table.print(out);
    }

    private void subscribers(List<String> args) throws IOException {
        String channel = parse(args, Set.of(), 1).positionalOrNull();
        List<SubscriberView> subscribers = channel == null
                ? bus().getSubscribers()
                : bus().getSubscribersForChannel(channel);
        TextTable table = new TextTable("ID", "CHANNEL", "TYPE", "PRIORITY", "MODE", "REF", "TREE",
                "DELIVERED", "FAILED", "SINCE");
        for (SubscriberView s : subscribers) {
            table.row(s.id(), s.channel(), s.eventType(), s.priority(), s.dispatchMode(), s.referenceType(),
                    s.hierarchical() ? "yes" : "no", s.deliveredCount(), s.failedCount(), dateTime(s.subscribedAt()));
        }
        table.print(out);
    }

    private void history(List<String> args) throws IOException {
        ParsedArgs parsed = parse(args, Set.of("-n"), 1);
        int limit = positiveInt(parsed.options().getOrDefault("-n", String.valueOf(DEFAULT_HISTORY_LIMIT)), "-n");
        TextTable table = new TextTable("SEQ", "TIME", "CHANNEL", "TYPE", "RECIPIENTS", "EVENT ID");
        for (EventView e : bus().getHistory(parsed.positionalOrNull(), 0, limit)) {
            table.row(e.sequence(), dateTime(e.publishedAt()), e.channel(), e.eventType(), e.recipientCount(), e.eventId());
        }
        table.print(out);
    }

    private void tail(List<String> args) throws IOException, InterruptedException {
        ParsedArgs parsed = parse(args, Set.of("--interval"), 1);
        long interval = positiveInt(parsed.options().getOrDefault("--interval", String.valueOf(DEFAULT_TAIL_INTERVAL_MS)), "--interval");
        String channel = parsed.positionalOrNull();
        EventBusAdminMXBean bus = bus();

        long cursor = bus.getSummary().lastSequence();
        out.println("Following " + (channel == null ? "all channels" : channel) + " — press Enter to stop.");
        while (!stopRequested()) {
            List<EventView> batch = bus.getHistory(channel, cursor, TAIL_BATCH_LIMIT);
            if (channel == null && !batch.isEmpty() && batch.get(0).sequence() > cursor + 1) {
                out.printf("... %d events skipped (published faster than the poll interval or evicted from history)%n",
                        batch.get(0).sequence() - cursor - 1);
            }
            for (EventView e : batch) {
                out.printf("%s  #%-6d %-30s %-24s -> %d%n",
                        time(e.publishedAt()), e.sequence(), e.channel(), e.eventType(), e.recipientCount());
                cursor = e.sequence();
            }
            Thread.sleep(interval);
        }
    }

    private void unsubscribe(List<String> args) throws IOException {
        String id = parse(args, Set.of(), 1).positionalOrNull();
        if (id == null) {
            throw new CliException("Usage: unsubscribe <subscription-id>");
        }
        out.println(bus().unsubscribe(id)
                ? "Subscription " + id + " ended."
                : "No live subscription with id " + id + ".");
    }

    private void clearHistory() throws IOException {
        bus().clearHistory();
        out.println("History cleared.");
    }

    private void help() {
        TextTable table = new TextTable("COMMAND", "DESCRIPTION");
        commands.values().forEach(c -> table.row(c.usage(), c.description()));
        table.print(out);
    }

    // === HELPERS ===

    private EventBusAdminMXBean bus() throws IOException {
        return client.bus(busName);
    }

    /** Consumes a pending input line, if any; any line (usually just Enter) stops {@code tail}. */
    private boolean stopRequested() throws IOException {
        if (in.ready()) {
            in.readLine();
            return true;
        }
        return false;
    }

    private static ParsedArgs parse(List<String> args, Set<String> optionNames, int maxPositional) {
        Map<String, String> options = new HashMap<>();
        List<String> positional = new ArrayList<>();
        for (int i = 0; i < args.size(); i++) {
            String arg = args.get(i);
            if (optionNames.contains(arg)) {
                if (i + 1 >= args.size()) {
                    throw new CliException("Option " + arg + " requires a value.");
                }
                options.put(arg, args.get(++i));
            } else if (arg.startsWith("-")) {
                throw new CliException("Unknown option: " + arg);
            } else {
                positional.add(arg);
            }
        }
        if (positional.size() > maxPositional) {
            throw new CliException("Too many arguments: " + positional);
        }
        return new ParsedArgs(options, positional);
    }

    private static int positiveInt(String value, String option) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed > 0) return parsed;
        } catch (NumberFormatException ignored) {
            // reported below
        }
        throw new CliException("Option " + option + " needs a positive number, got '" + value + "'.");
    }

    private static String dateTime(String iso) {
        return iso == null ? null : DATE_TIME.format(Instant.parse(iso));
    }

    private static String time(String iso) {
        return TIME.format(Instant.parse(iso));
    }
}
