package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Event;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-10-08
 */
public interface EventHandler<E extends Event> {

    /**
     * Get the channel name that this handler handles events from
     * @return The channel name
     */
    @Nonnull
    String getChannelName();

    /**
     * Gets the name of this handler.  Handler names are used to form group names for listeners.
     * Handler names should be determined by the usage context of the handler.  For a given context they
     * should be the same over different runs of the application (in other words, handler names should not
     * be randomly created on a run).
     * @return The handler name for this handler.
     */
    @Nonnull
    String getHandlerName();

    /**
     * Gets the class of events that this handler handles
     * @return The class of requests
     */
    Class<E> getEventClass();

    /**
     * Handle an event
     * @param event The event to be handled
     */
     void handleEvent(E event);
}
