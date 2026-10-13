package ae.eventbus.core.request;

import ae.eventbus.core.channel.Channel;

/**
 * Thrown when a request is dispatched to a channel that has no registered request handlers.
 */
public class NoResponderException extends RuntimeException {

    private final Channel channel;

    public NoResponderException(Channel channel) {
        super(String.format("No active request handlers found listening on channel: %s", channel));
        this.channel = channel;
    }

    public Channel getChannel() {
        return channel;
    }
}