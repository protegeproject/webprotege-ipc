package edu.stanford.protege.webprotege.ipc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import edu.stanford.protege.webprotege.common.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A utility for asynchronously awaiting {@link Event}s keyed by an identifier.
 * <p>
 * This class provides a mechanism to:
 * <ul>
 *     <li>Register interest in an event with a specific key and asynchronously await its arrival.</li>
 *     <li>Complete awaiting futures when events are observed.</li>
 *     <li>Optionally buffer early-arriving events that occur before any awaiter is registered.</li>
 *     <li>Propagate failures to awaiting clients via exceptional completion.</li>
 * </ul>
 *
 * <p><b>Remarks:</b></p>
 * <ul>
 *     <li>The key {@code K} is typically the {@code EventId} or another unique identifier
 *         that correlates an emitted event with an awaiting request.</li>
 *     <li>The event type {@code E} must be a concrete subclass of {@link Event} —
 *         callers are expected to work with a specific event type rather than the abstract base type.</li>
 *     <li>This awaiter is typically used in combination with an {@code EventHandler}.
 *         In such setups, the same {@code EventAwaiter} instance is registered as a Spring component
 *         and injected both into the event handler (to call {@link #complete(Object, Event)})
 *         and into the client or listener code (to call {@link #await(Object, Duration)}).</li>
 *     <li>This pattern is common in <b>Sagas</b>, where long-running workflows must wait
 *         for specific events before proceeding to the next step.</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * @Component
 * public class MySaga {
 *
 *     private final EventAwaiter<String, MyEvent> awaiter;
 *
 *     public MySaga(EventAwaiter<String, MyEvent> awaiter) {
 *         this.awaiter = awaiter;
 *     }
 *
 *     public void runSaga() {
 *         // Await a response event with a timeout
 *         awaiter.await("job-123", Duration.ofSeconds(5))
 *                .thenAccept(ev -> {
 *                    System.out.println("Saga continued with event: " + ev);
 *                })
 *                .exceptionally(ex -> {
 *                    System.err.println("Saga failed: " + ex.getMessage());
 *                    return null;
 *                });
 *     }
 * }
 *
 * @Component
 * public class MyEventHandler implements EventHandler<MyEvent> {
 *
 *     private final EventAwaiter<String, MyEvent> awaiter;
 *
 *     public MyEventHandler(EventAwaiter<String, MyEvent> awaiter) {
 *         this.awaiter = awaiter;
 *     }
 *
 *     @Override
 *     public void handle(MyEvent event) {
 *         // Complete the awaiting future when the event is observed
 *         awaiter.complete(event.getId(), event);
 *     }
 * }
 * }</pre>
 *
 * <h2>Early Event Buffering</h2>
 * <p>
 * If an event arrives before {@link #await(Object, Duration)} is called, it is
 * buffered and delivered immediately when the awaiter is later invoked for that key:
 * </p>
 * <pre>{@code
 * EventAwaiter<String, MyEvent> awaiter = new EventAwaiter<>();
 *
 * // Event arrives before anyone is awaiting
 * awaiter.complete("job-123", new MyEvent(...));
 *
 * // Later, when awaiting, the future completes immediately
 * awaiter.await("job-123", Duration.ofSeconds(5))
 *        .thenAccept(ev -> System.out.println("Received early event: " + ev));
 * }</pre>
 *
 * <h2>Capacity &amp; Expiration</h2>
 * <p>
 * This implementation bounds and expires its internal maps to avoid unbounded growth:
 * </p>
 * <ul>
 *   <li><b>Pending map</b> (key → {@code CompletableFuture&lt;E&gt;}): bounded by size and
 *       expires entries after a configurable TTL. When a pending entry is evicted due to size or TTL,
 *       its future is <i>cancelled</i> to signal downstream awaiters.</li>
 *   <li><b>Early map</b> (key → {@code E}): bounded by size and expires entries after a configurable TTL.
 *       Expired early events are simply discarded.</li>
 * </ul>
 * <p>
 * Defaults are provided by the no-arg constructor and can be overridden via the
 * {@linkplain #EventAwaiter(long, Duration, long, Duration) configurable constructor}.
 * </p>
 *
 * @param <K> The type of the event key used to correlate awaiters with events.
 *            Typically an {@code EventId}.
 * @param <E> The type of the event being awaited. Must be a specific, concrete
 *            subclass of {@link Event}.
 */
public class EventAwaiter<K, E extends Event> {

    private static final Logger logger = LoggerFactory.getLogger(EventAwaiter.class);

    // ==== Configuration defaults ====
    private static final long DEFAULT_PENDING_MAX_SIZE = 100_000L;
    private static final Duration DEFAULT_PENDING_TTL = Duration.ofHours(6);
    private static final long DEFAULT_EARLY_MAX_SIZE = 100_000L;
    private static final Duration DEFAULT_EARLY_TTL = Duration.ofHours(24);

    private final Cache<K, CompletableFuture<E>> pending;
    private final Cache<K, E> early;

    /**
     * Creates an {@code EventAwaiter} with sensible defaults for capacity and expiration:
     * <ul>
     *   <li>Pending: max size 100,000; TTL 6 hours; pending futures are cancelled on eviction.</li>
     *   <li>Early:   max size 100,000; TTL 24 hours.</li>
     * </ul>
     */
    public EventAwaiter() {
        this(DEFAULT_EARLY_MAX_SIZE, DEFAULT_EARLY_TTL, DEFAULT_PENDING_MAX_SIZE, DEFAULT_PENDING_TTL);
    }

    /**
     * Creates an {@code EventAwaiter} with custom capacity and expiration settings.
     *
     * @param earlyMaxSize   maximum number of buffered early events to retain
     * @param earlyTtl       how long to retain a buffered early event before it expires
     * @param pendingMaxSize maximum number of pending awaits to retain
     * @param pendingTtl     how long to retain a pending await before it expires;
     *                       if a pending entry expires or is evicted due to size, its future is cancelled
     */
    public EventAwaiter(long earlyMaxSize,
                        Duration earlyTtl,
                        long pendingMaxSize,
                        Duration pendingTtl) {

        this.pending = Caffeine.newBuilder()
                .maximumSize(pendingMaxSize)
                .expireAfterWrite(pendingTtl)
                .removalListener((K key, CompletableFuture<E> fut, RemovalCause cause) -> {
                    if (fut == null) {
                        return;
                    }
                    boolean wasDone = fut.isDone();
                    if (cause == RemovalCause.EXPIRED || cause == RemovalCause.SIZE) {
                        boolean cancelled = fut.cancel(true);
                        // Log specifically when a listener (pending await) was evicted
                        logger.warn(
                                "EventAwaiter pending listener evicted [key={}, cause={}, wasDone={}, cancelled={}]",
                                key, cause, wasDone, cancelled
                        );
                    } else {
                        // Other causes: REPLACED, EXPLICIT, COLLECTED
                        logger.debug(
                                "EventAwaiter pending entry removed [key={}, cause={}, wasDone={}]",
                                key, cause, wasDone
                        );
                    }
                })
                .build();

        this.early = Caffeine.newBuilder()
                .maximumSize(earlyMaxSize)
                .expireAfterWrite(earlyTtl)
                .removalListener((K key, E ev, RemovalCause cause) -> {
                    if (cause == RemovalCause.EXPIRED || cause == RemovalCause.SIZE) {
                        // Not a listener, but useful to see when a buffered event is dropped
                        logger.debug("EventAwaiter early event evicted [key={}, cause={}]", key, cause);
                    }
                })
                .build();
    }

    private static <E extends Event> CompletableFuture<E> completedCast(E ev) {
        CompletableFuture<E> f = new CompletableFuture<>();
        f.complete(ev);
        return f;
    }

    /**
     * Awaits the event corresponding to the given event key.
     * <p>
     * If an event for the specified key has already arrived early, this method
     * returns a completed future immediately. Otherwise, a new (or existing)
     * {@link CompletableFuture} is returned that completes when the event is
     * supplied via {@link #complete(Object, Event)}.
     * <p>
     * If a timeout is specified, the returned future will complete exceptionally
     * with a {@link java.util.concurrent.TimeoutException} if the event does not
     * arrive within the given duration.
     *
     * @param eventKey The key that identifies the awaited event. Typically an {@code EventId}.
     * @param timeout  The maximum duration to wait for the event. If {@code null}, waits indefinitely.
     * @return A {@link CompletableFuture} that completes with the awaited event, or exceptionally on timeout.
     */
    public CompletableFuture<E> await(K eventKey, Duration timeout) {
        // Fast path: event already arrived
        E earlyEvent = early.asMap().remove(eventKey);
        if (earlyEvent != null) {
            return completedCast(earlyEvent);
        }

        // Create or reuse a pending future for this request
        CompletableFuture<E> cf = pending.asMap().computeIfAbsent(eventKey, __ -> new CompletableFuture<>());

        // Cleanup no matter how it completes
        cf.whenComplete((__, ___) -> pending.asMap().remove(eventKey, cf));

        CompletableFuture<E> withTimeout = (timeout == null)
                ? cf
                : cf.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS); // Java 9+

        return withTimeout;
    }

    /**
     * Completes the future associated with the given event key with the supplied event.
     * <p>
     * If an awaiter exists, its future is completed immediately. If no awaiter exists yet,
     * the event is buffered as an "early" event and delivered immediately when
     * {@link #await(Object, Duration)} is later called.
     *
     * @param eventKey The key of the event to complete.
     * @param event    The concrete event to complete with.
     */
    public void complete(K eventKey, E event) {
        CompletableFuture<E> cf = pending.asMap().get(eventKey);
        if (cf != null) {
            cf.complete(event);
        } else {
            // Nobody is awaiting yet: buffer it.
            early.asMap().putIfAbsent(eventKey, event);
        }
    }

    /**
     * Completes the future associated with the given event key exceptionally.
     * <p>
     * This can be used to propagate errors to awaiting clients if a failure event is observed.
     *
     * @param eventKey The key of the event to complete exceptionally.
     * @param t        The throwable to complete the future with.
     * @return {@code true} if a future was found and completed exceptionally,
     *         {@code false} if no awaiter was present for the given key.
     */
    public boolean completeExceptionally(K eventKey, Throwable t) {
        CompletableFuture<E> cf = pending.asMap().remove(eventKey);
        if (cf != null) {
            return cf.completeExceptionally(t);
        }
        return false;
    }

    /**
     * Cancels an outstanding await for the given event key and clears any buffered early event.
     * <p>
     * If a caller is currently awaiting the event identified by {@code eventKey}, the corresponding
     * {@link CompletableFuture} (if present) is completed exceptionally with a
     * {@link java.util.concurrent.CancellationException} by invoking {@link CompletableFuture#cancel(boolean)}.
     * <p>
     * Additionally, if an event had already arrived early for the same key (and is buffered in the
     * internal early-event map), that buffered event is removed to prevent immediate delivery on a
     * subsequent {@link #await(Object, Duration)} call.
     *
     * @param eventKey The key of the event to cancel and clear.
     * @return {@code true} if either a pending await was cancelled or a buffered early event was removed;
     *         {@code false} if there was nothing to cancel or clear for the given key.
     */
    public boolean cancel(K eventKey) {
        boolean affected = false;

        // Cancel and remove any pending future
        CompletableFuture<E> cf = pending.asMap().remove(eventKey);
        if (cf != null) {
            affected |= cf.cancel(true);
        }

        // Remove any early-buffered event so it doesn't auto-complete a later await
        E removedEarly = early.asMap().remove(eventKey);
        if (removedEarly != null) {
            affected = true;
        }

        return affected;
    }
}