package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Event;

import javax.annotation.Nonnull;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-10
 */
public interface GenericEventHandler {

    String ALL_EVENTS_CHANNEL = "webprotege.events.all";

    /**
     * Gets the name of this handler.
     * Handler names should be determined by the usage context of the handler.  For a given context they
     * should be the same over different runs of the application (in other words, handler names should not
     * be randomly created on a run).
     * @return The handler name for this handler.
     */
    @Nonnull
    String getHandlerName();

    default void handlerSubscribed() {

    }

    void handleEventRecord(EventRecord eventRecord);
}
