package ae.eventbus.core.routing;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Thread-safe channel-to-subscribers index shared by the built-in routers.
 *
 * <p>Registration and removal run inside {@link ConcurrentHashMap#compute} so that a channel's
 * list is never dropped while another thread is adding to it. Reads iterate a
 * {@link CopyOnWriteArrayList} snapshot and take no locks.</p>
 */
final class ChannelSubscriberMap {

    private final ConcurrentHashMap<Channel, CopyOnWriteArrayList<Subscriber<?>>> subscribers = new ConcurrentHashMap<>();

    void add(Subscriber<?> subscriber) {
        subscribers.compute(subscriber.getChannel(), (channel, list) -> {
            CopyOnWriteArrayList<Subscriber<?>> target = (list != null) ? list : new CopyOnWriteArrayList<>();
            target.add(subscriber);
            return target;
        });
    }

    void remove(Subscriber<?> subscriber) {
        subscribers.computeIfPresent(subscriber.getChannel(), (channel, list) -> {
            list.remove(subscriber);
            return list.isEmpty() ? null : list;
        });
    }

    /** Returns the live subscriber list for the channel (possibly containing invalid entries), never {@code null}. */
    List<Subscriber<?>> get(Channel channel) {
        List<Subscriber<?>> list = subscribers.get(channel);
        return (list != null) ? list : List.of();
    }

    /** Returns only the currently valid subscribers of the channel. */
    List<Subscriber<?>> getValid(Channel channel) {
        return get(channel).stream().filter(Subscriber::isValid).toList();
    }

    Set<Channel> channels() {
        return Collections.unmodifiableSet(subscribers.keySet());
    }
}
