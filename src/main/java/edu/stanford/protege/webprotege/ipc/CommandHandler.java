package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-06
 */
public interface CommandHandler<Q extends Request<R>, R extends Response> {

    /**
     * Get the channel name that this handler handles requests from
     * @return The channel name
     */
    @Nonnull
    String getChannelName();

    /**
     * Gets the class of requests that this handler handles
     * @return The class of requests
     */
    Class<Q> getRequestClass();

    /**
     * Handle a request and return a response
     * @param request
     * @return
     */
    Mono<R> handleRequest(Q request);
}
