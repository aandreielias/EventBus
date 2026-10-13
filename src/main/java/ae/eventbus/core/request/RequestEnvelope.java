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

/**
 * Special event carrying a request payload and managing completion promises.
 * Supports:
 * <ul>
 *   <li>Single-responder (first Response wins)</li>
 *   <li>Multi-responder </li>
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

    public void submitResponse(RES response) {
        singleFuture.complete(response);
        collectedResponses.add(response);
        if (remainingResponders.decrementAndGet() <= 0) {
            allFuture.complete(Collections.unmodifiableList(new ArrayList<>(collectedResponses)));
        }
    }

    public void submitFailure(Throwable throwable) {
        singleFuture.completeExceptionally(throwable);
        if (remainingResponders.decrementAndGet() <= 0) {
            if (collectedResponses.isEmpty()) {
                allFuture.completeExceptionally(throwable);
            } else {
                allFuture.complete(Collections.unmodifiableList(new ArrayList<>(collectedResponses)));
            }
        }
    }
}