package ae.eventbus.core.registry;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;

/**
 * High-performance no-op implementation of {@link EventRegistry} (Null Object Pattern).
 * Disables historical tracking and event recording to minimize memory and CPU overhead.
 */
public final class NoOpEventRegistry implements EventRegistry {

    @Override
    public void recordPublication(Event event, Channel channel, int recipientCount) {
        // No-op
    }

    @Override
    public Set<Channel> getActiveChannels() {
        return Collections.emptySet();
    }

    @Override
    public List<SubscriberInfo> getAllSubscribers() {
        return Collections.emptyList();
    }

    @Override
    public List<SubscriberInfo> getSubscribersForChannel(Channel channel) {
        return Collections.emptyList();
    }

    @Override
    public List<EventRecord> getHistory() {
        return Collections.emptyList();
    }

    @Override
    public List<EventRecord> getHistoryForChannel(Channel channel) {
        return Collections.emptyList();
    }

    @Override
    public void clearHistory() {
        // No-op
    }

    @Override
    public void registerChannel(Channel channel) {
    }

    @Override
    public Optional<ChannelInfo> getChannelInfo(Channel channel) {
        return Optional.empty();
    }

    @Override
    public List<ChannelInfo> getAllChannelInfos() {
        return Collections.emptyList();
    }
}