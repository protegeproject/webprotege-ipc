package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Event;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Function;

/**
 * Generic EventHandler that completes an {@link EventAwaiter} when an event is observed.
 * The channel and handler name are part of this class's identity and must be provided
 * by overriding {@link #getChannelName()} and {@link #getHandlerName()}.
 *
 * @param <K> The correlation key type (typically EventId or String).
 * @param <E> The concrete event type.
 */
public abstract class AwaiterCompletingEventHandler<K, E extends Event> implements EventHandler<E> {

    private final Class<E> eventClass;

    private final EventAwaiter<K, E> awaiter;

    private final Function<E, K> keyExtractor;

    /**
     * @param eventClass   Concrete event class this handler accepts
     * @param awaiter      Shared awaiter to complete
     * @param keyExtractor Function to extract the correlation key (e.g., EventId) from the event
     */
    protected AwaiterCompletingEventHandler(@Nonnull Class<E> eventClass,
                                            @Nonnull EventAwaiter<K, E> awaiter,
                                            @Nonnull Function<E, K> keyExtractor) {
        this.eventClass = Objects.requireNonNull(eventClass);
        this.awaiter = Objects.requireNonNull(awaiter);
        this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }

    /** Identity: override in subclasses to declare the fixed channel name this handler listens on. */
    @Nonnull
    @Override
    public abstract String getChannelName();

    /** Identity: override in subclasses to declare the stable handler name used for listener groups. */
    @Nonnull
    @Override
    public abstract String getHandlerName();

    @Override
    public Class<E> getEventClass() {
        return eventClass;
    }

    /** Completes the awaiter for the extracted key. */
    @Override
    public void handleEvent(E event) {
        K key = keyExtractor.apply(event);
        awaiter.complete(key, event);
    }

    /** Default ExecutionContext-aware path delegates to the simple variant. Override if you need context. */
    @Override
    public void handleEvent(E event, ExecutionContext executionContext) {
        handleEvent(event);
    }
}

