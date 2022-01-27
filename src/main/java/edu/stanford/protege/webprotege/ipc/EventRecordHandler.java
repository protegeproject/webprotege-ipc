package edu.stanford.protege.webprotege.ipc;

import javax.annotation.Nonnull;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-01-08
 */
public interface EventRecordHandler {

    /**
     * Gets the name of this handler.  Handler names are used to form group names for Kafka listeners.
     * Handler names should be determined by the usage context of the handler.  For a given context they
     * should be the same over different runs of the application (in other words, handler names should not
     * be randomly created on a run).
     * @return The handler name for this handler.
     */
    @Nonnull
    String getHandlerName();

    void handleEventRecord(EventRecord eventRecord);
}
