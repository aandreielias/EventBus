package ae.eventbus.core.channel;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Represents a hierarchical channel as an immutable Value Object.
 * <p>
 * Channels represent destinations or topics within the event bus. A channel is structured
 * as an ordered sequence of path segments (e.g., {@code ["ui", "button", "click"]}).
 * This internal representation decouples hierarchy resolution from delimiter formatting.
 * </p>
 * <p>
 * This class is thread-safe and immutable.
 * </p>
 */
public final class Channel implements Comparable<Channel> {

    /**
     * Default delimiter used when serializing or parsing hierarchical channel strings.
     */
    public static final String DEFAULT_SEPARATOR = "/";

    private final List<String> segments;

    /**
     * Lazily cached parent channel to avoid redundant object allocations on repeated calls.
     */
    private volatile Channel cachedParent;
    private volatile boolean parentResolved = false;

    /**
     * Lazily cached string representation using the default separator.
     */
    private volatile String cachedToString;

    /**
     * Private constructor to enforce validation and creation via static factory methods.
     *
     * @param segments the list of path segments
     * @throws IllegalArgumentException if the segments list is null, empty, or contains blank segments
     */
    private Channel(List<String> segments) {
        if (segments == null || segments.isEmpty()) {
            throw new IllegalArgumentException("A channel must contain at least one segment.");
        }
        List<String> cleanSegments = new java.util.ArrayList<>(segments.size());
        for (String segment : segments) {
            if (segment == null || segment.trim().isEmpty()) {
                throw new IllegalArgumentException("Channel segments must neither be null nor empty.");
            }
            cleanSegments.add(segment.trim());
        }
        // Defensive copy guarantees immutability
        this.segments = List.copyOf(cleanSegments);
    }

    /**
     * Creates a {@link Channel} from individual path segments.
     * <p>Example: {@code Channel.of("ui", "button", "click")}</p>
     *
     * @param segments array of path segments
     * @return a new immutable {@link Channel} instance
     * @throws IllegalArgumentException if segments is null or empty
     */
    public static Channel of(String... segments) {
        if (segments == null || segments.length == 0) {
            throw new IllegalArgumentException("At least one segment must be provided.");
        }
        return new Channel(Arrays.asList(segments));
    }

    /**
     * Parses a delimited string into a {@link Channel} using the {@link #DEFAULT_SEPARATOR} ({@code "/"}).
     * Leading, trailing, and duplicate delimiters are ignored (e.g., {@code "/ui/button/" -> "ui/button"}).
     *
     * @param path the path string to parse
     * @return a new {@link Channel} instance
     * @throws IllegalArgumentException if the path is null, blank, or has no valid segments
     */
    public static Channel parse(String path) {
        return parse(path, DEFAULT_SEPARATOR);
    }

    /**
     * Parses a delimited string into a {@link Channel} using a custom delimiter string.
     *
     * @param path      the path string to parse
     * @param separator the delimiter string (e.g., {@code "."} or {@code "/"})
     * @return a new {@link Channel} instance
     * @throws IllegalArgumentException if the path or separator is null or blank
     */
    public static Channel parse(String path, String separator) {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("Path must neither be null nor blank.");
        }
        if (separator == null || separator.isEmpty()) {
            throw new IllegalArgumentException("Separator must neither be null nor empty.");
        }

        String[] rawSegments = path.split(Pattern.quote(separator));
        List<String> cleanSegments = Arrays.stream(rawSegments)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();

        if (cleanSegments.isEmpty()) {
            throw new IllegalArgumentException("The path contains no valid segments: " + path);
        }

        return new Channel(cleanSegments);
    }

    /**
     * Returns an unmodifiable list of the segments comprising this channel.
     *
     * @return list of segments
     */
    public List<String> getSegments() {
        return segments;
    }

    /**
     * Returns the depth of the channel hierarchy, which equals the number of segments.
     *
     * @return depth of this channel
     */
    public int getDepth() {
        return segments.size();
    }

    /**
     * Determines whether this channel is a root channel (depth == 1).
     *
     * @return {@code true} if this channel has no parent, otherwise {@code false}
     */
    public boolean isRoot() {
        return segments.size() == 1;
    }

    /**
     * Returns the immediate parent channel if one exists.
     * <p>
     * For example, {@code "ui/button/click"} yields {@code Optional.of("ui/button")}.
     * For root channels (e.g., {@code "ui"}), {@link Optional#empty()} is returned.
     * The result is lazily cached to avoid redundant object creation.
     * </p>
     *
     * @return an {@link Optional} containing the parent channel, or empty if this is a root channel
     */
    public Optional<Channel> getParent() {
        if (isRoot()) {
            return Optional.empty();
        }
        if (!parentResolved) {
            synchronized (this) {
                if (!parentResolved) {
                    List<String> parentSegments = segments.subList(0, segments.size() - 1);
                    this.cachedParent = new Channel(parentSegments);
                    this.parentResolved = true;
                }
            }
        }
        return Optional.ofNullable(cachedParent);
    }

    /**
     * Checks if this channel is an ancestor (direct or indirect parent) of another channel.
     * <p>
     * For example, {@code Channel.parse("ui").isParentOf(Channel.parse("ui/button"))} returns {@code true}.
     * </p>
     *
     * @param other the channel to compare against
     * @return {@code true} if this channel is an ancestor of {@code other}, otherwise {@code false}
     */
    public boolean isParentOf(Channel other) {
        if (other == null || other.getDepth() <= this.getDepth()) {
            return false;
        }
        for (int i = 0; i < this.segments.size(); i++) {
            if (!this.segments.get(i).equals(other.segments.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if this channel is a descendant (direct or indirect child) of another channel.
     *
     * @param other the candidate ancestor channel
     * @return {@code true} if this channel is a child of {@code other}, otherwise {@code false}
     */
    public boolean isChildOf(Channel other) {
        return other != null && other.isParentOf(this);
    }

    /**
     * Formats the channel into a path string using the specified delimiter.
     *
     * @param separator the delimiter string to join segments with
     * @return formatted channel string
     */
    public String toString(String separator) {
        if (separator == null || separator.isEmpty()) {
            throw new IllegalArgumentException("Separator cannot be null or empty.");
        }
        return String.join(separator, segments);
    }

    /**
     * Formats the channel into a path string using {@link #DEFAULT_SEPARATOR}.
     * The value is lazily cached for performance.
     *
     * @return formatted channel path string
     */
    @Override
    public String toString() {
        if (cachedToString == null) {
            cachedToString = toString(DEFAULT_SEPARATOR);
        }
        return cachedToString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Channel other)) return false;
        return segments.equals(other.segments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segments);
    }

    @Override
    public int compareTo(Channel o) {
        return this.toString().compareTo(o.toString());
    }
}