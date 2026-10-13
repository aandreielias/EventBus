package ae.eventbus.core.validation;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.Event;

import java.util.regex.Pattern;

/**
 * Default structural validator enforcing channel naming and depth constraints.
 *
 * <ul>
 *   <li>Payloads must not be {@code null}.</li>
 *   <li>Channel depth is limited to {@value #DEFAULT_MAX_CHANNEL_DEPTH} segments unless configured otherwise.</li>
 *   <li>Segments may only contain {@code [a-zA-Z0-9_.-]} and never the bus's channel separator,
 *       so every channel round-trips through its string path.</li>
 * </ul>
 */
public final class DefaultEventValidator implements EventValidator {

    public static final int DEFAULT_MAX_CHANNEL_DEPTH = 16;
    private static final Pattern VALID_SEGMENT_PATTERN = Pattern.compile("^[a-zA-Z0-9_\\-\\.]+$");

    private final int maxChannelDepth;
    private final String channelSeparator;

    /**
     * @param maxChannelDepth  maximum number of segments
     * @param channelSeparator separator segments must not contain
     */
    public DefaultEventValidator(int maxChannelDepth, String channelSeparator) {
        if (maxChannelDepth <= 0) {
            throw new IllegalArgumentException("Max channel depth must be greater than zero.");
        }
        if (channelSeparator == null || channelSeparator.isEmpty()) {
            throw new IllegalArgumentException("Channel separator must neither be null nor empty.");
        }
        this.maxChannelDepth = maxChannelDepth;
        this.channelSeparator = channelSeparator;
    }

    /** Validator for the default separator {@code "/"}. */
    public DefaultEventValidator(int maxChannelDepth) {
        this(maxChannelDepth, Channel.DEFAULT_SEPARATOR);
    }

    /** Validator with the default depth and separator. */
    public DefaultEventValidator() {
        this(DEFAULT_MAX_CHANNEL_DEPTH);
    }

    @Override
    public void validateChannel(Channel channel) throws ValidationException {
        if (channel == null) {
            throw new ValidationException("Channel must not be null.");
        }
        if (channel.getDepth() > maxChannelDepth) {
            throw new ValidationException(String.format(
                    "Channel depth %d exceeds maximum allowed depth of %d: %s",
                    channel.getDepth(), maxChannelDepth, channel
            ));
        }
        for (String segment : channel.getSegments()) {
            if (!VALID_SEGMENT_PATTERN.matcher(segment).matches() || segment.contains(channelSeparator)) {
                throw new ValidationException(String.format(
                        "Channel segment '%s' in channel '%s' contains illegal characters. Only alphanumeric, dashes, dots, and underscores are allowed, and never the separator '%s'.",
                        segment, channel, channelSeparator
                ));
            }
        }
    }

    @Override
    public void validateEvent(Event event) throws ValidationException {
        if (event == null) {
            throw new ValidationException("Event must not be null.");
        }
        if (event.getId() == null) {
            throw new ValidationException("Event must contain a valid non-null UUID.");
        }
        if (event.getTimestamp() == null) {
            throw new ValidationException("Event must contain a valid non-null Instant timestamp.");
        }
        if (event.getChannels() == null || event.getChannels().isEmpty()) {
            throw new ValidationException("Event must belong to at least one destination channel.");
        }
        for (Channel channel : event.getChannels()) {
            validateChannel(channel);
        }
    }

    @Override
    public <T> void validatePayload(Channel channel, T payload) throws ValidationException {
        validateChannel(channel);
        if (payload == null) {
            throw new ValidationException("Payload must not be null.");
        }
    }
}