package ae.eventbus.core.request;

/**
 * Functional interface for processing a request and computing a response.
 *
 * @param <REQ> the incoming request payload type
 * @param <RES> the outgoing response type
 */
@FunctionalInterface
public interface RequestHandler<REQ, RES> {

    /**
     * Processes the request and returns a response.
     *
     * @param request the incoming request
     * @return the computed response
     * @throws Exception if processing fails
     */
    RES handle(REQ request) throws Exception;
}