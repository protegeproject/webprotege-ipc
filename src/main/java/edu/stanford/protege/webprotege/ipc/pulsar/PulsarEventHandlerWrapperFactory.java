package edu.stanford.protege.webprotege.ipc.pulsar;

import edu.stanford.protege.webprotege.ipc.EventHandler;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-03
 */
public interface PulsarEventHandlerWrapperFactory {

    PulsarEventHandlerWrapper<?> create(EventHandler<?> handler);
}
