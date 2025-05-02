package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.UserId;
import edu.stanford.protege.webprotege.ipc.util.CorrelationMDCUtil;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.UUID;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-19
 */
public record ExecutionContext(@Nonnull UserId userId, @Nonnull  String jwt, @Nonnull String correlationId) {

    public ExecutionContext() {
        this(UserId.getGuest(), "", CorrelationMDCUtil.getCorrelationId() == null ? UUID.randomUUID().toString() : CorrelationMDCUtil.getCorrelationId());
    }

    public ExecutionContext {
        Objects.requireNonNull(userId, "userId cannot be null");
        Objects.requireNonNull(jwt, "jwt cannot be null");
    }
}
