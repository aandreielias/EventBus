package ae.eventbus.core.routing;

import java.util.List;
import java.util.Set;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Strategy interface defining how subscribers are matched to events and channels.
 */
public interface EventRouter {

    /**
     * Registers a subscriber with this router.
     *
     * @param subscriber the subscriber to register
     */
    void register(Subscriber<?> subscriber);

    /**
     * Removes a subscriber from this router.
     *
     * @param subscriber the subscriber to remove
     */
    void deregister(Subscriber<?> subscriber);

    /**
     * Resolves all eligible subscribers for the given event and channel.
     * Results are <strong>not guaranteed to be sorted</strong>; ordering by
     * {@link ae.eventbus.core.subscription.Priority} is the caller's responsibility.
     *
     * @param event   the event being published
     * @param channel the target channel
     * @return matched subscribers in unspecified order
     */
    List<Subscriber<?>> resolveSubscribers(Event event, Channel channel);

    /**
     * Returns all channels currently holding at least one registered subscriber.
     *
     * @return unmodifiable set of active channels
     */
    Set<Channel> getActiveChannels();

    /**
     * Returns all active subscribers registered to the given channel.
     *
     * @param channel target channel
     * @return list of active subscribers
     */
    List<Subscriber<?>> getSubscribersForChannel(Channel channel);
}
