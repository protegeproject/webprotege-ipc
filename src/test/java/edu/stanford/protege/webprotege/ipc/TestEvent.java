package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.stanford.protege.webprotege.common.EventId;
import edu.stanford.protege.webprotege.common.ProjectEvent;
import edu.stanford.protege.webprotege.common.ProjectId;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;

@JsonTypeName("TestEvent")
public class TestEvent implements ProjectEvent {

    public static final String CHANNEL = "the.test.event.channel";

    private final ProjectId projectId;

    private EventId eventId;

    @JsonCreator
    public TestEvent(@JsonProperty("eventId") String eventId, @JsonProperty("projectId") String projectId) {
        this.eventId = new EventId(eventId);
        this.projectId = new ProjectId(projectId);
    }


    @Nonnull
    @Override
    public EventId eventId() {
        return eventId;
    }

    @Override
    public String getChannel() {
        return CHANNEL;
    }

    @NotNull
    @Override
    public ProjectId projectId() {
        return projectId;
    }

    public ProjectId getProjectId() {
        return projectId;
    }

    public EventId getEventId() {
        return eventId;
    }

    public void setEventId(EventId eventId) {
        this.eventId = eventId;
    }
}