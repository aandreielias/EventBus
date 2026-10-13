package ae.eventbus.core.admin;

import ae.eventbus.core.EventBus;
import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.registry.ChannelInfo;
import ae.eventbus.core.registry.EventRecord;
import ae.eventbus.core.registry.EventRegistry;
import ae.eventbus.core.registry.SubscriberInfo;

import javax.management.JMException;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Exposes an {@link EventBus} as a JMX MXBean so it can be inspected and controlled from a terminal
 * (bundled CLI), {@code jconsole} or any other JMX client.
 *
 * <pre>{@code
 * EventBus bus = EventBusBuilder.create().build();
 * EventBusAdmin admin = EventBusAdmin.register(bus, "club-server", AdminAccess.READ_ONLY);
 * ...
 * admin.close();   // unregisters the MBean
 * }</pre>
 *
 * <p>The bean is registered as {@code ae.eventbus:type=EventBus,name=<busName>}. Introspection relies on
 * the bus's registry, so a bus built with {@code withNoOpRegistry()} reports no channels, subscribers or
 * history.</p>
 *
 * <p><strong>Security:</strong> {@link AdminAccess} only limits what <em>this</em> bean allows. It is
 * not a security boundary: whoever can connect over JMX can also use the JVM's built-in management
 * beans (heap dumps, thread dumps, system properties). Treat JMX access as full access to the JVM.
 * When enabling remote JMX always configure authentication, an access file and TLS
 * ({@code com.sun.management.jmxremote.authenticate}, {@code .access.file}, {@code .ssl}).
 * Local attach (CLI {@code --pid}) is limited to the OS user running the JVM; it starts the JVM's
 * local JMX agent, which stays running after the CLI exits.</p>
 *
 * <p>Closing the bus does not unregister the bean (it then reports the bus as closed); call
 * {@link #close()} to unregister it.</p>
 */
public final class EventBusAdmin implements EventBusAdminMXBean, AutoCloseable {

    /** JMX domain of all EventBus MBeans. */
    public static final String DOMAIN = "ae.eventbus";

    private static final Pattern VALID_NAME = Pattern.compile("[A-Za-z0-9_.-]+");

    private final EventBus bus;
    private final String busName;
    private final AdminAccess access;
    private final MBeanServer server;
    private final ObjectName objectName;

    private EventBusAdmin(EventBus bus, String busName, AdminAccess access, MBeanServer server) {
        this.bus = Objects.requireNonNull(bus, "EventBus must not be null.");
        this.access = Objects.requireNonNull(access, "AdminAccess must not be null.");
        this.server = Objects.requireNonNull(server, "MBeanServer must not be null.");
        this.busName = busName;
        this.objectName = objectName(busName);
    }

    /** Registers the bus on the platform MBean server. */
    public static EventBusAdmin register(EventBus bus, String busName, AdminAccess access) {
        return register(ManagementFactory.getPlatformMBeanServer(), bus, busName, access);
    }

    /**
     * Registers the bus on the given MBean server.
     *
     * @param busName unique name within the JVM; letters, digits, {@code _ . -}
     * <p>A name still held by a bus that has since been closed is taken over, so an application
     * can rebuild its bus under the same name without unregistering the old one first.</p>
     *
     * @throws IllegalArgumentException if the name is invalid
     * @throws IllegalStateException    if an open bus is already registered under that name
     */
    public static EventBusAdmin register(MBeanServer server, EventBus bus, String busName, AdminAccess access) {
        EventBusAdmin admin = new EventBusAdmin(bus, busName, access, server);
        try {
            if (server.isRegistered(admin.objectName)
                    && JMX.newMXBeanProxy(server, admin.objectName, EventBusAdminMXBean.class).getSummary().closed()) {
                server.unregisterMBean(admin.objectName);
            }
            server.registerMBean(admin, admin.objectName);
        } catch (JMException e) {
            throw new IllegalStateException("Could not register EventBus '" + busName + "' with JMX: " + e.getMessage(), e);
        }
        return admin;
    }

    /** Returns the JMX object name a bus with the given name is registered under. */
    public static ObjectName objectName(String busName) {
        if (busName == null || !VALID_NAME.matcher(busName).matches()) {
            throw new IllegalArgumentException("Bus name must match " + VALID_NAME.pattern() + ": " + busName);
        }
        try {
            return new ObjectName(DOMAIN + ":type=EventBus,name=" + busName);
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /** Pattern matching every registered EventBus MBean. */
    public static ObjectName queryPattern() {
        try {
            return new ObjectName(DOMAIN + ":type=EventBus,*");
        } catch (MalformedObjectNameException e) {
            throw new IllegalStateException(e);
        }
    }

    public ObjectName getObjectName() {
        return objectName;
    }

    // === READ OPERATIONS ===

    @Override
    public BusSummary getSummary() {
        EventRegistry registry = bus.getRegistry();
        List<EventRecord> history = registry.getHistory();
        long publications = registry.getAllChannelInfos().stream().mapToLong(ChannelInfo::getPublicationCount).sum();
        long lastSequence = history.isEmpty() ? 0 : history.get(history.size() - 1).getSequence();
        return new BusSummary(busName, access.name(), bus.getChannelSeparator(), bus.isClosed(),
                knownChannels().size(), registry.getAllSubscribers().size(), publications,
                history.size(), lastSequence);
    }

    @Override
    public List<ChannelView> getChannels() {
        EventRegistry registry = bus.getRegistry();
        return knownChannels().entrySet().stream()
                .map(entry -> {
                    ChannelInfo.ChannelInfoSnapshot info = entry.getValue();
                    int subscribers = registry.getSubscribersForChannel(info.channel()).size();
                    return new ChannelView(entry.getKey(), subscribers, info.publicationCount(),
                            iso(info.createdAt()), iso(info.lastFiredAt()));
                })
                .toList();
    }

    @Override
    public List<SubscriberView> getSubscribers() {
        return toViews(bus.getRegistry().getAllSubscribers());
    }

    @Override
    public List<SubscriberView> getSubscribersForChannel(String channelPath) {
        return toViews(bus.getRegistry().getSubscribersForChannel(parse(channelPath)));
    }

    @Override
    public List<EventView> getHistory(String channelPath, long afterSequence, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be greater than zero.");
        }
        Channel filter = (channelPath == null || channelPath.isBlank()) ? null : parse(channelPath);
        List<EventView> matching = bus.getRegistry().getHistory().stream()
                .filter(record -> record.getSequence() > afterSequence)
                .filter(record -> filter == null || filter.equals(record.getChannel()) || record.getChannel().isChildOf(filter))
                .map(this::toView)
                .toList();
        return matching.subList(Math.max(0, matching.size() - limit), matching.size());
    }

    // === CONTROL OPERATIONS ===

    @Override
    public boolean unsubscribe(String subscriptionId) {
        requireWriteAccess("unsubscribe");
        UUID id;
        try {
            id = UUID.fromString(Objects.requireNonNull(subscriptionId, "Subscription id must not be null."));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Not a valid subscription id: " + subscriptionId);
        }
        return bus.unsubscribe(id);
    }

    @Override
    public void clearHistory() {
        requireWriteAccess("clearHistory");
        bus.getRegistry().clearHistory();
    }

    /** Unregisters the MBean. Idempotent; does not close the bus. */
    @Override
    public void close() {
        try {
            if (server.isRegistered(objectName)) {
                server.unregisterMBean(objectName);
            }
        } catch (JMException e) {
            throw new IllegalStateException("Could not unregister EventBus '" + busName + "' from JMX.", e);
        }
    }

    // === HELPERS ===

    private void requireWriteAccess(String operation) {
        if (access != AdminAccess.READ_WRITE) {
            throw new SecurityException("Operation '" + operation + "' is not permitted: bus '" + busName
                    + "' is exposed " + access + ".");
        }
    }

    /** Channels with statistics plus channels that have subscribers but no statistics (registry cap reached), keyed by path. */
    private Map<String, ChannelInfo.ChannelInfoSnapshot> knownChannels() {
        EventRegistry registry = bus.getRegistry();
        Map<String, ChannelInfo.ChannelInfoSnapshot> channels = new TreeMap<>();
        for (ChannelInfo info : registry.getAllChannelInfos()) {
            channels.put(format(info.getChannel()), info.toSnapshot());
        }
        Set<Channel> active = registry.getActiveChannels();
        for (Channel channel : active) {
            channels.putIfAbsent(format(channel), new ChannelInfo.ChannelInfoSnapshot(channel, null, null, 0));
        }
        return channels;
    }

    private List<SubscriberView> toViews(List<SubscriberInfo> subscribers) {
        return subscribers.stream()
                .sorted(Comparator.comparing((SubscriberInfo s) -> format(s.channel())).thenComparing(s -> s.id().toString()))
                .map(s -> new SubscriberView(s.id().toString(), format(s.channel()), s.eventType().getSimpleName(),
                        s.priority().name(), s.dispatchMode().name(), s.referenceType().name(),
                        s.handlesChannelChildren(), s.deliveredCount(), s.failedCount(), iso(s.subscribedAt())))
                .toList();
    }

    private EventView toView(EventRecord record) {
        return new EventView(record.getSequence(), record.getEventId().toString(), record.getEventType(),
                format(record.getChannel()), iso(record.getPublishedAt()), record.getRecipientCount());
    }

    private Channel parse(String channelPath) {
        return Channel.parse(channelPath, bus.getChannelSeparator());
    }

    private String format(Channel channel) {
        return channel.toString(bus.getChannelSeparator());
    }

    private static String iso(Instant instant) {
        return instant == null ? null : instant.toString();
    }
}
