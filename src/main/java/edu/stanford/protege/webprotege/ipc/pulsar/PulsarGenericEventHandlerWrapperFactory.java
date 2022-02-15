package edu.stanford.protege.webprotege.ipc.pulsar;

import edu.stanford.protege.webprotege.ipc.GenericEventHandler;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-11
 */
public interface PulsarGenericEventHandlerWrapperFactory {

    PulsarGenericEventHandlerWrapper create(GenericEventHandler handler);
}
