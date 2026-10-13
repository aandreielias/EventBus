package ae.eventbus.core.routing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.Subscriber;

/**
 * O(1) router that matches subscribers only on the exact target channel.
 * Does not traverse parent channels.
 * Results are returned unsorted — priority ordering is the caller's responsibility.
 */
public final class ExactMatchRouter implements EventRouter {

    private final ConcurrentHashMap<Channel, CopyOnWriteArrayList<Subscriber<?>>> subscriberMap = new ConcurrentHashMap<>();

    @Override
    public void register(Subscriber<?> subscriber) {
        if (subscriber == null) return;
        subscriberMap.computeIfAbsent(subscriber.getChannel(), k -> new CopyOnWriteArrayList<>())
                     .add(subscriber);
    }

    @Override
    public void deregister(Subscriber<?> subscriber) {
        if (subscriber == null) return;
        CopyOnWriteArrayList<Subscriber<?>> list = subscriberMap.get(subscriber.getChannel());
        if (list != null) {
            list.remove(subscriber);
            if (list.isEmpty()) {
                subscriberMap.remove(subscriber.getChannel(), list);
            }
        }
    }

    @Override
    public List<Subscriber<?>> resolveSubscribers(Event event, Channel channel) {
        CopyOnWriteArrayList<Subscriber<?>> list = subscriberMap.get(channel);
        if (list == null || list.isEmpty()) {
            return Collections.emptyList();
        }

        List<Subscriber<?>> result = new ArrayList<>();
        for (Subscriber<?> sub : list) {
            if (!sub.isValid()) {
                deregister(sub);
            } else if (event == null || sub.canHandle(event)) {
                result.add(sub);
            }
        }

        return Collections.unmodifiableList(result);
    }

    @Override
    public Set<Channel> getActiveChannels() {
        return Collections.unmodifiableSet(subscriberMap.keySet());
    }

    @Override
    public List<Subscriber<?>> getSubscribersForChannel(Channel channel) {
        CopyOnWriteArrayList<Subscriber<?>> list = subscriberMap.get(channel);
        if (list == null) return Collections.emptyList();
        return list.stream().filter(Subscriber::isValid).toList();
    }
}
