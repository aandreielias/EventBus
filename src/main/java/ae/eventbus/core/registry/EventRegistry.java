package ae.eventbus.core.registry;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Diagnostic and audit contract capturing full lifecycle history:
 * Channel creation, subscription/unsubscription events, and publication timestamps.
 */
public interface EventRegistry {

    /**
     * Records an event publication in the chronological history and updates channel activity.
     *
     * @param event          the published event
     * @param channel        the target channel
     * @param recipientCount number of subscribers that received the event
     */
    void recordPublication(Event event, Channel channel, int recipientCount);

    /**
     * Explicitly registers or records discovery of a channel.
     *
     * @param channel the channel to track
     */
    void registerChannel(Channel channel);

    /**
     * Retrieves audit info for a specific channel, including creation and last fired timestamps.
     *
     * @param channel target channel
     * @return optional containing {@link ChannelInfo} if known
     */
    Optional<ChannelInfo> getChannelInfo(Channel channel);

    /**
     * Retrieves audit info for all known channels.
     *
     * @return list of channel info records
     */
    List<ChannelInfo> getAllChannelInfos();

    /**
     * Retrieves all currently active channels known to the event bus.
     *
     * @return set of channels
     */
    Set<Channel> getActiveChannels();

    /**
     * Retrieves metadata for all currently registered subscribers (including subscribedAt timestamps).
     *
     * @return list of subscriber metadata
     */
    List<SubscriberInfo> getAllSubscribers();

    /**
     * Retrieves metadata for subscribers registered to a specific channel.
     *
     * @param channel target channel
     * @return list of matching subscriber metadata
     */
    List<SubscriberInfo> getSubscribersForChannel(Channel channel);

    /**
     * Retrieves the chronological publication history.
     *
     * @return ordered list of event records (oldest to newest)
     */
    List<EventRecord> getHistory();

    /**
     * Retrieves the chronological publication history filtered by channel.
     *
     * @param channel target channel
     * @return ordered list of matching event records
     */
    List<EventRecord> getHistoryForChannel(Channel channel);

    /**
     * Clears all recorded event history.
     */
    void clearHistory();
}