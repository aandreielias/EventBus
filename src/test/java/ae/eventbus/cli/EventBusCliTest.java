package ae.eventbus.cli;

import ae.eventbus.core.EventBus;
import ae.eventbus.core.EventBusBuilder;
import ae.eventbus.core.admin.AdminAccess;
import ae.eventbus.core.admin.EventBusAdmin;
import ae.eventbus.core.subscription.Subscription;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventBusCliTest {

    private MBeanServer server;
    private EventBus bus;
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errors = new ByteArrayOutputStream();

    @BeforeEach
    void setUp() {
        server = MBeanServerFactory.newMBeanServer();
        bus = EventBusBuilder.create().build();
    }

    @AfterEach
    void tearDown() {
        bus.close();
    }

    private Commands commands(String input) {
        return new Commands(AdminClient.of(server), null, new PrintStream(output, true),
                new BufferedReader(new StringReader(input)));
    }

    private String run(String... command) throws Exception {
        commands("").execute(List.of(command));
        return output.toString();
    }

    @Test
    void channelsAndSubscribersAreListed() throws Exception {
        EventBusAdmin.register(server, bus, "club", AdminAccess.READ_ONLY);
        bus.subscribe("bookings/created", String.class, e -> {});
        bus.publish("bookings/created", "court-1");

        assertThat(run("channels")).contains("CHANNEL", "bookings/created");
        assertThat(run("subscribers", "bookings/created")).contains("String", "NORMAL", "SYNCHRONOUS");
        assertThat(run("history", "-n", "5")).contains("String", "bookings/created");
        assertThat(run("info")).contains("Bus:            club", "READ_ONLY");
    }

    @Test
    void unsubscribeWorksWithReadWriteAccess() throws Exception {
        EventBusAdmin.register(server, bus, "club", AdminAccess.READ_WRITE);
        Subscription subscription = bus.subscribe("x", String.class, e -> {});

        assertThat(run("unsubscribe", subscription.getId().toString())).contains("ended");
        assertThat(subscription.isActive()).isFalse();
    }

    @Test
    void unsubscribeIsDeniedWithReadOnlyAccess() {
        EventBusAdmin.register(server, bus, "club", AdminAccess.READ_ONLY);
        Subscription subscription = bus.subscribe("x", String.class, e -> {});

        assertThatThrownBy(() -> run("unsubscribe", subscription.getId().toString()))
                .isInstanceOf(SecurityException.class);
    }

    @Test
    void severalBusesRequireAnExplicitChoice() throws Exception {
        try (EventBus other = EventBusBuilder.create().build()) {
            EventBusAdmin.register(server, bus, "club", AdminAccess.READ_ONLY);
            EventBusAdmin.register(server, other, "payments", AdminAccess.READ_ONLY);
            Commands commands = commands("");

            assertThatThrownBy(() -> commands.execute(List.of("info")))
                    .isInstanceOf(CliException.class).hasMessageContaining("--bus");

            commands.execute(List.of("use", "payments"));
            commands.execute(List.of("info"));
            assertThat(output.toString()).contains("Bus:            payments");
        }
    }

    @Test
    void shellRunsCommandsReportsErrorsAndExits() throws Exception {
        EventBusAdmin.register(server, bus, "club", AdminAccess.READ_ONLY);
        String input = "channels\nbogus\nexit\nchannels\n";
        BufferedReader in = new BufferedReader(new StringReader(input));
        AdminClient client = AdminClient.of(server);
        Commands commands = new Commands(client, null, new PrintStream(output, true), in);

        int code = EventBusCli.shell(client, commands, in, new PrintStream(output, true), new PrintStream(errors, true));

        assertThat(code).isZero();
        assertThat(output.toString()).contains("Connected. Buses: [club]", "(none)");
        assertThat(errors.toString()).contains("Unknown command 'bogus'");
    }

    @Test
    void tailStopsWhenEnterIsPressed() throws Exception {
        EventBusAdmin.register(server, bus, "club", AdminAccess.READ_ONLY);

        commands("\n").execute(List.of("tail", "--interval", "10"));

        assertThat(output.toString()).contains("Following all channels");
    }

    @Test
    void argumentsAreValidated() {
        assertThat(CliArguments.parse(new String[]{"--pid", "42", "--bus", "club", "history", "-n", "5"}))
                .satisfies(a -> {
                    assertThat(a.pid()).isEqualTo(42L);
                    assertThat(a.busName()).isEqualTo("club");
                    assertThat(a.command()).containsExactly("history", "-n", "5");
                });
        assertThat(CliArguments.parse(new String[]{"--host", "db:9010"}).jmxUrl())
                .isEqualTo("service:jmx:rmi:///jndi/rmi://db:9010/jmxrmi");
        assertThatThrownBy(() -> CliArguments.parse(new String[]{"channels"}))
                .isInstanceOf(CliException.class);
        assertThatThrownBy(() -> CliArguments.parse(new String[]{"--pid", "abc"}))
                .isInstanceOf(CliException.class);
    }

    @Test
    void usageErrorsExitWithCodeTwo() {
        int code = EventBusCli.run(new String[]{"--nope"}, System.in, new PrintStream(output), new PrintStream(errors));

        assertThat(code).isEqualTo(EventBusCli.USAGE_ERROR);
        assertThat(errors.toString()).contains("Unknown option: --nope", "Usage: ebus");
    }

    @Test
    void invalidCommandOptionsAreRejected() {
        EventBusAdmin.register(server, bus, "club", AdminAccess.READ_ONLY);

        assertThatThrownBy(() -> run("history", "-n", "0")).isInstanceOf(CliException.class);
        assertThatThrownBy(() -> run("channels", "--bogus")).isInstanceOf(CliException.class);
    }
}
