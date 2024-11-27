package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Event;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-01-31
 */
public interface EventDispatcher {
    void dispatchEvent(Event event, ExecutionContext executionContext);

    void dispatchEvent(Event event);
}
