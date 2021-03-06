package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.EventId;
import edu.stanford.protege.webprotege.common.ProjectId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-01-08
 */
public record EventRecord(EventId eventId,
                          long timestamp,
                          String eventType,
                          byte [] eventPayload,
                          @Nullable ProjectId projectId) {


    @Nonnull
    public Optional<ProjectId> getProjectId() {
        return Optional.ofNullable(projectId());
    }

    @Nullable
    public ProjectId projectId() {
        return projectId;
    }
}
