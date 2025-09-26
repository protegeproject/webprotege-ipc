package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.common.EventId;

import java.util.function.Function;

/**
 * Utility methods for working with Event keys.
 */
public final class EventKeyExtractors {

    private EventKeyExtractors() {
        // Prevent instantiation
    }

    /**
     * Returns a function that extracts the {@link EventId} from any {@link Event}.
     *
     * <p>This can be used with {@link AwaiterCompletingEventHandler}
     * to hook up the {@link EventAwaiter} with minimal boilerplate.</p>
     *
     * @param <E> The concrete event type
     * @return A function that extracts the EventId from an Event
     */
    public static <E extends Event> Function<E, EventId> byEventId() {
        return Event::eventId;
    }
}
