package ae.eventbus.core.routing;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Router supporting parent-channel inheritance.
 *
 * <p>When an event is dispatched to {@code "ui/button/click"}, subscribers are collected from:</p>
 * <ol>
 *   <li>The exact channel {@code "ui/button/click"}.</li>
 *   <li>Any ancestor channel (e.g. {@code "ui/button"}, {@code "ui"}) whose subscriber has
 *       {@code handlesChannelChildren = true}.</li>
 * </ol>
 * <p>Results are returned unsorted — priority ordering is the caller's responsibility.</p>
 */
public final class HierarchicalRouter implements EventRouter {

    private final ChannelSubscriberMap subscriberMap = new ChannelSubscriberMap();

    @Override
    public void register(Subscriber<?> subscriber) {
        if (subscriber != null) subscriberMap.add(subscriber);
    }

    @Override
    public void deregister(Subscriber<?> subscriber) {
        if (subscriber != null) subscriberMap.remove(subscriber);
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

        return List.copyOf(collected);
    }

    private void collectSubscribersForChannel(Event event, Channel ch, boolean mustHandleChildren, Set<Subscriber<?>> target) {
        for (Subscriber<?> sub : subscriberMap.get(ch)) {
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
        return subscriberMap.channels();
    }

    @Override
    public List<Subscriber<?>> getSubscribersForChannel(Channel channel) {
        return subscriberMap.getValid(channel);
    }
}
