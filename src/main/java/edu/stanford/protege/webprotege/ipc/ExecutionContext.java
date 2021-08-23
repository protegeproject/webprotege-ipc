package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.UserId;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-19
 */
public record ExecutionContext(UserId userId) {

    public ExecutionContext() {
        this(UserId.getGuest());
    }
}
