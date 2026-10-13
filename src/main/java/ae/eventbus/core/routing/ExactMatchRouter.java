package ae.eventbus.core.routing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.Subscriber;

/**
 * O(1) router that matches subscribers only on the exact target channel.
 * Does not traverse parent channels.
 * Results are returned unsorted — priority ordering is the caller's responsibility.
 */
public final class ExactMatchRouter implements EventRouter {

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
        List<Subscriber<?>> result = new ArrayList<>();
        for (Subscriber<?> sub : subscriberMap.get(channel)) {
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
        return subscriberMap.channels();
    }

    @Override
    public List<Subscriber<?>> getSubscribersForChannel(Channel channel) {
        return subscriberMap.getValid(channel);
    }
}
