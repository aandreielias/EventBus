package ae.eventbus.core.admin;

import ae.eventbus.core.EventBus;
import ae.eventbus.core.EventBusBuilder;
import ae.eventbus.core.admin.EventBusAdminMXBean.EventView;
import ae.eventbus.core.subscription.Subscription;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventBusAdminTest {

    private MBeanServer server;
    private EventBus bus;

    @BeforeEach
    void setUp() {
        server = MBeanServerFactory.newMBeanServer();
        bus = EventBusBuilder.create().build();
    }

    @AfterEach
    void tearDown() {
        bus.close();
    }

    /** Reads the bean through a real MXBean proxy, so the open-type mapping is exercised too. */
    private EventBusAdminMXBean proxy(String name) {
        return JMX.newMXBeanProxy(server, EventBusAdmin.objectName(name), EventBusAdminMXBean.class);
    }

    @Test
    void exposesChannelsSubscribersAndHistory() {
        EventBusAdmin.register(server, bus, "club", AdminAccess.READ_ONLY);
        bus.subscribe("members/joined", String.class, e -> {});
        bus.publish("members/joined", "alice");
        bus.publish("members/left", "bob");

        EventBusAdminMXBean admin = proxy("club");

        assertThat(admin.getSummary().subscriberCount()).isEqualTo(1);
        assertThat(admin.getSummary().publicationCount()).isEqualTo(2);
        assertThat(admin.getChannels()).extracting(EventBusAdminMXBean.ChannelView::channel)
                .containsExactly("members/joined", "members/left");
        assertThat(admin.getSubscribersForChannel("members/joined")).singleElement()
                .satisfies(s -> assertThat(s.deliveredCount()).isEqualTo(1));
        assertThat(admin.getHistory("members", 0, 10)).extracting(EventView::channel)
                .containsExactly("members/joined", "members/left");
        assertThat(admin.getHistory(null, 0, 1)).extracting(EventView::channel)
                .containsExactly("members/left");
    }

    @Test
    void historyCursorReturnsOnlyNewerRecords() {
        EventBusAdmin.register(server, bus, "club", AdminAccess.READ_ONLY);
        bus.publish("a", "1");
        long cursor = proxy("club").getSummary().lastSequence();
        bus.publish("a", "2");

        assertThat(proxy("club").getHistory("", cursor, 100)).hasSize(1);
    }

    @Test
    void readOnlyAccessRejectsControlOperations() {
        EventBusAdmin.register(server, bus, "club", AdminAccess.READ_ONLY);
        Subscription subscription = bus.subscribe("x", String.class, e -> {});

        assertThatThrownBy(() -> proxy("club").unsubscribe(subscription.getId().toString()))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> proxy("club").clearHistory())
                .isInstanceOf(SecurityException.class);
        assertThat(subscription.isActive()).isTrue();
    }

    @Test
    void readWriteAccessAllowsUnsubscribeAndClearHistory() {
        EventBusAdmin.register(server, bus, "club", AdminAccess.READ_WRITE);
        Subscription subscription = bus.subscribe("x", String.class, e -> {});
        bus.publish("x", "1");

        assertThat(proxy("club").unsubscribe(subscription.getId().toString())).isTrue();
        proxy("club").clearHistory();

        assertThat(subscription.isActive()).isFalse();
        assertThat(bus.getHistory()).isEmpty();
    }

    @Test
    void closeUnregistersAndDuplicateNamesAreRejected() {
        EventBusAdmin admin = EventBusAdmin.register(server, bus, "club", AdminAccess.READ_ONLY);

        assertThatThrownBy(() -> EventBusAdmin.register(server, bus, "club", AdminAccess.READ_ONLY))
                .isInstanceOf(IllegalStateException.class);

        admin.close();
        admin.close();
        assertThat(server.isRegistered(EventBusAdmin.objectName("club"))).isFalse();
    }

    @Test
    void invalidBusNamesAreRejected() {
        assertThatThrownBy(() -> EventBusAdmin.objectName("bad name,type=x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nameOfAClosedBusCanBeReused() {
        EventBusAdmin.register(server, bus, "club", AdminAccess.READ_ONLY);
        bus.close();

        try (EventBus rebuilt = EventBusBuilder.create().build()) {
            EventBusAdmin.register(server, rebuilt, "club", AdminAccess.READ_ONLY);
            assertThat(proxy("club").getSummary().closed()).isFalse();
        }
    }
}
