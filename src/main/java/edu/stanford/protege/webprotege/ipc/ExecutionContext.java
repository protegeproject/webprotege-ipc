package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.UserId;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-19
 */
public record ExecutionContext(@Nonnull UserId userId, @Nonnull  String jwt) {

    public ExecutionContext() {
        this(UserId.getGuest(), "");
    }

    public ExecutionContext {
        Objects.requireNonNull(userId, "userId cannot be null");
        Objects.requireNonNull(jwt, "jwt cannot be null");
    }
}
