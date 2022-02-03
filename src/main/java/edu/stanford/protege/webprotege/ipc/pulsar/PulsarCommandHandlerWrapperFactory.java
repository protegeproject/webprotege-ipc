package edu.stanford.protege.webprotege.ipc.pulsar;

import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.CommandHandler;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-02
 */
public interface PulsarCommandHandlerWrapperFactory {


    <Q extends Request<R>, R extends Response> PulsarCommandHandlerWrapper<Q, R> create(CommandHandler<Q, R> handler);
}
