package ae.eventbus.core.registry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;
import ae.eventbus.core.routing.EventRouter;
import ae.eventbus.core.subscription.Subscriber;

/**
 * Thread-safe audit registry that records channel lifecycle, publication history,
 * and subscriber metadata.
 *
 * <p>History is maintained as a bounded ring buffer: once {@code maxHistorySize} is
 * reached, the oldest entry is evicted on each new publication.</p>
 *
 * <p>{@link ChannelInfo} instances returned by this registry are <strong>live views</strong>.
 * Call {@link ChannelInfo#toSnapshot()} for a consistent point-in-time copy.</p>
 */
public final class DefaultEventRegistry implements EventRegistry {

    public static final int DEFAULT_MAX_HISTORY_SIZE = 1_000;
    public static final int DEFAULT_MAX_CHANNELS = 10_000;

    private final EventRouter router;
    private final int maxHistorySize;
    private final int maxChannels;

    private final ConcurrentHashMap<Channel, ChannelInfo> channelInfoMap = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<EventRecord> history = new ConcurrentLinkedQueue<>();
    private final AtomicInteger historySize = new AtomicInteger(0);
    private final AtomicLong sequence = new AtomicLong(0);

    public DefaultEventRegistry(EventRouter router, int maxHistorySize, int maxChannels) {
        this.router = Objects.requireNonNull(router, "EventRouter must not be null.");
        if (maxHistorySize <= 0) {
            throw new IllegalArgumentException("Max history size must be greater than zero.");
        }
        if (maxChannels <= 0) {
            throw new IllegalArgumentException("Max channels must be greater than zero.");
        }
        this.maxHistorySize = maxHistorySize;
        this.maxChannels = maxChannels;
    }

    public DefaultEventRegistry(EventRouter router, int maxHistorySize) {
        this(router, maxHistorySize, DEFAULT_MAX_CHANNELS);
    }

    public DefaultEventRegistry(EventRouter router) {
        this(router, DEFAULT_MAX_HISTORY_SIZE, DEFAULT_MAX_CHANNELS);
    }

    @Override
    public void registerChannel(Channel channel) {
        if (channel != null && (channelInfoMap.size() < maxChannels || channelInfoMap.containsKey(channel))) {
            channelInfoMap.computeIfAbsent(channel, ChannelInfo::new);
        }
    }

    @Override
    public void recordPublication(Event event, Channel channel, int recipientCount) {
        if (channelInfoMap.size() < maxChannels || channelInfoMap.containsKey(channel)) {
            channelInfoMap.computeIfAbsent(channel, ChannelInfo::new).recordFired();
        }

        history.offer(new EventRecord(sequence.incrementAndGet(), event, channel, recipientCount));
        if (historySize.incrementAndGet() > maxHistorySize) {
            if (history.poll() != null) {
                historySize.decrementAndGet();
            }
        }
    }

    @Override
    public Optional<ChannelInfo> getChannelInfo(Channel channel) {
        return Optional.ofNullable(channelInfoMap.get(channel));
    }

    @Override
    public List<ChannelInfo> getAllChannelInfos() {
        return List.copyOf(channelInfoMap.values());
    }

    @Override
    public Set<Channel> getActiveChannels() {
        return router.getActiveChannels();
    }

    @Override
    public List<SubscriberInfo> getAllSubscribers() {
        List<SubscriberInfo> subscribers = new ArrayList<>();
        for (Channel channel : router.getActiveChannels()) {
            for (Subscriber<?> sub : router.getSubscribersForChannel(channel)) {
                subscribers.add(toInfo(sub));
            }
        }
        return Collections.unmodifiableList(subscribers);
    }

    @Override
    public List<SubscriberInfo> getSubscribersForChannel(Channel channel) {
        return router.getSubscribersForChannel(channel).stream()
                .map(this::toInfo)
                .toList();
    }

    @Override
    public List<EventRecord> getHistory() {
        return List.copyOf(history);
    }

    @Override
    public List<EventRecord> getHistoryForChannel(Channel channel) {
        return history.stream()
                .filter(record -> record.getChannel().equals(channel))
                .toList();
    }

    @Override
    public void clearHistory() {
        history.clear();
        historySize.set(0);
    }

    private SubscriberInfo toInfo(Subscriber<?> sub) {
        return new SubscriberInfo(
                sub.getId(),
                sub.getChannel(),
                sub.getEventType(),
                sub.getPriority(),
                sub.getDispatchMode(),
                sub.getReferenceType(),
                sub.isHandlesChannelChildren(),
                sub.isValid(),
                sub.getSubscribedAt(),
                sub.getUnsubscribedAt(),
                sub.getDeliveredCount(),
                sub.getFailedCount()
        );
    }
}
