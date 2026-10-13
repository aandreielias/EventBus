package ae.eventbus.core.routing;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Router supporting parent-channel inheritance.
 *
 * <p>When an event is dispatched to {@code "ui/button/click"}, subscribers are collected from:
 * <ol>
 *   <li>The exact channel {@code "ui/button/click"}.</li>
 *   <li>Any ancestor channel (e.g. {@code "ui/button"}, {@code "ui"}) whose subscriber has
 *       {@code handlesChannelChildren = true}.</li>
 * </ol>
 * Results are returned unsorted — priority ordering is the caller's responsibility.
 * </p>
 */
public final class HierarchicalRouter implements EventRouter {

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
        Set<Subscriber<?>> collected = new HashSet<>();

        collectSubscribersForChannel(event, channel, false, collected);

        Optional<Channel> parent = channel.getParent();
        while (parent.isPresent()) {
            Channel parentChannel = parent.get();
            collectSubscribersForChannel(event, parentChannel, true, collected);
            parent = parentChannel.getParent();
        }

        if (collected.isEmpty()) {
            return Collections.emptyList();
        }

        return List.copyOf(collected);
    }

    private void collectSubscribersForChannel(Event event, Channel ch, boolean mustHandleChildren, Set<Subscriber<?>> target) {
        CopyOnWriteArrayList<Subscriber<?>> list = subscriberMap.get(ch);
        if (list == null) return;

        for (Subscriber<?> sub : list) {
            if (!sub.isValid()) {
                deregister(sub);
                continue;
            }
            if ((!mustHandleChildren || sub.isHandlesChannelChildren()) && (event == null || sub.canHandle(event))) {
                target.add(sub);
            }
        }
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
