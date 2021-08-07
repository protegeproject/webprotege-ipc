package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-04
 */
public interface Event {

    @JsonIgnore
    String getChannelName();
}
