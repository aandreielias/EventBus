package ae.eventbus.cli;

import ae.eventbus.core.admin.EventBusAdmin;
import ae.eventbus.core.admin.EventBusAdminMXBean;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.Console;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * JMX connection to a JVM hosting one or more EventBus MBeans.
 */
final class AdminClient implements AutoCloseable {

    private final MBeanServerConnection connection;
    private final JMXConnector connector;

    private AdminClient(MBeanServerConnection connection, JMXConnector connector) {
        this.connection = connection;
        this.connector = connector;
    }

    /** Wraps an existing connection (used for in-process access and tests). */
    static AdminClient of(MBeanServerConnection connection) {
        return new AdminClient(connection, null);
    }

    /** Connects to the target described by the arguments: local attach by pid, or remote JMX URL. */
    static AdminClient connect(CliArguments args) throws IOException {
        String url = args.pid() != null ? localAgentAddress(args.pid()) : args.jmxUrl();
        Map<String, Object> environment = args.user() == null
                ? Map.of()
                : Map.of(JMXConnector.CREDENTIALS, new String[]{args.user(), password(args.user())});
        JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(url), environment);
        return new AdminClient(connector.getMBeanServerConnection(), connector);
    }

    /** Names of all buses registered in the target JVM, sorted. */
    List<String> busNames() throws IOException {
        return connection.queryNames(EventBusAdmin.queryPattern(), null).stream()
                .map(name -> name.getKeyProperty("name"))
                .sorted()
                .toList();
    }

    /**
     * Returns a proxy for the named bus, or for the only bus when {@code busName} is {@code null}.
     *
     * @throws CliException if the bus does not exist or the choice is ambiguous
     */
    EventBusAdminMXBean bus(String busName) throws IOException {
        List<String> names = busNames();
        if (names.isEmpty()) {
            throw new CliException("No EventBus is registered in the target JVM. Register one with EventBusAdmin.register(...).");
        }
        String selected = busName;
        if (selected == null) {
            if (names.size() > 1) {
                throw new CliException("Several buses are registered " + names + "; choose one with --bus or 'use <name>'.");
            }
            selected = names.get(0);
        } else if (!names.contains(selected)) {
            throw new CliException("No bus named '" + selected + "'. Registered buses: " + names);
        }
        ObjectName objectName = EventBusAdmin.objectName(selected);
        return JMX.newMXBeanProxy(connection, objectName, EventBusAdminMXBean.class);
    }

    @Override
    public void close() throws IOException {
        if (connector != null) {
            connector.close();
        }
    }

    private static String localAgentAddress(long pid) throws IOException {
        VirtualMachine vm;
        try {
            vm = VirtualMachine.attach(String.valueOf(pid));
        } catch (AttachNotSupportedException e) {
            throw new CliException("Cannot attach to process " + pid + ": " + e.getMessage()
                    + " (is it a JVM running as the same OS user?)", e);
        }
        try {
            return vm.startLocalManagementAgent();
        } finally {
            vm.detach();
        }
    }

    private static String password(String user) {
        String fromEnv = System.getenv("EBUS_PASSWORD");
        if (fromEnv != null) {
            return fromEnv;
        }
        Console console = System.console();
        if (console == null) {
            throw new CliException("No console to prompt for a password; set EBUS_PASSWORD.");
        }
        char[] password = console.readPassword("Password for %s: ", user);
        return password == null ? "" : new String(password);
    }
}
