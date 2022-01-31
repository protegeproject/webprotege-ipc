package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;

import java.util.concurrent.CompletableFuture;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-03
 */
public interface CommandExecutor<Q extends Request<R>, R extends Response> {

    CompletableFuture<R> execute(Q request, ExecutionContext executionContext);
}
