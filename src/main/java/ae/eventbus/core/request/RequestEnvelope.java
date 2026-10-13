package ae.eventbus.core.request;

import ae.eventbus.core.channel.Channel;
import ae.eventbus.core.event.AbstractEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Special event carrying a request payload and managing completion promises.
 * Supports:
 * <ul>
 *   <li>Single-responder: the first successful response wins; the request fails only if every responder fails</li>
 *   <li>Multi-responder: all successful responses; fails only if every responder fails</li>
 *   <li>Targeted requests (addressed to a specific subscriber)</li>
 * </ul>
 *
 * @param <REQ> request type
 * @param <RES> expected response type
 */
public final class RequestEnvelope<REQ, RES> extends AbstractEvent {

    private final REQ request;
    private final Class<RES> responseType;
    private final UUID targetSubscriberId; // Nullable: if set, only this subscriber may answer

    // Single Responder Promise
    private final CompletableFuture<RES> singleFuture = new CompletableFuture<>();

    // Multi Responder Promise (Scatter-Gather)
    private final CompletableFuture<List<RES>> allFuture = new CompletableFuture<>();
    private final List<RES> collectedResponses = new CopyOnWriteArrayList<>();
    private final AtomicInteger remainingResponders = new AtomicInteger(0);
    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();

    /**
     * Constructs a broadcast request (any or all subscribers on the channel may answer).
     */
    public RequestEnvelope(REQ request, Channel channel, Class<RES> responseType) {
        this(request, channel, responseType, null);
    }

    /**
     * Constructs a targeted request addressed to a specific subscriber ID.
     */
    public RequestEnvelope(REQ request, Channel channel, Class<RES> responseType, UUID targetSubscriberId) {
        super(channel);
        this.request = Objects.requireNonNull(request, "Request payload must not be null.");
        this.responseType = Objects.requireNonNull(responseType, "ResponseType must not be null.");
        this.targetSubscriberId = targetSubscriberId;
    }

    public REQ getRequest() {
        return request;
    }

    public Class<RES> getResponseType() {
        return responseType;
    }

    public Optional<UUID> getTargetSubscriberId() {
        return Optional.ofNullable(targetSubscriberId);
    }

    public CompletableFuture<RES> getSingleFuture() {
        return singleFuture;
    }

    public CompletableFuture<List<RES>> getAllFuture() {
        return allFuture;
    }

    public void setExpectedResponders(int count) {
        if (count <= 0) {
            allFuture.complete(List.of());
            return;
        }
        remainingResponders.set(count);
    }

    /**
     * Records a successful response from one responder.
     * A non-null response that is not an instance of {@link #getResponseType()} is recorded
     * as a {@link ClassCastException} failure instead, so callers never receive a mistyped value.
     */
    public void submitResponse(RES response) {
        if (response != null && !responseType.isInstance(response)) {
            submitFailure(new ClassCastException(String.format(
                    "Request handler on channel '%s' returned %s, but the requester expects %s.",
                    getPrimaryChannel(), response.getClass().getName(), responseType.getName())));
            return;
        }
        collectedResponses.add(response);
        singleFuture.complete(response);
        responderFinished();
    }

    /** Records a failure from one responder. Other responders may still answer successfully. */
    public void submitFailure(Throwable throwable) {
        firstFailure.compareAndSet(null, throwable);
        responderFinished();
    }

    /** Completes the futures once the last responder has answered or failed. */
    private void responderFinished() {
        if (remainingResponders.decrementAndGet() > 0) {
            return;
        }
        if (collectedResponses.isEmpty()) {
            cancel(firstFailure.get());
        } else {
            allFuture.complete(Collections.unmodifiableList(new ArrayList<>(collectedResponses)));
        }
    }

    /**
     * Fails both futures immediately, regardless of outstanding responders.
     * Used when the bus shuts down while the request is still pending.
     */
    public void cancel(Throwable cause) {
        singleFuture.completeExceptionally(cause);
        allFuture.completeExceptionally(cause);
    }

    /** Returns {@code true} once both the single and the scatter-gather futures are complete. */
    public boolean isDone() {
        return singleFuture.isDone() && allFuture.isDone();
    }
}
