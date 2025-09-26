package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Event;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class EventAwaiterTest {

    private static final String KEY = "job-123";

    @Test
    void awaitThenComplete_CompletesFuture() throws Exception {
        EventAwaiter<String, Event> awaiter = new EventAwaiter<>();
        Event event = mock(Event.class);

        CompletableFuture<Event> future = awaiter.await(KEY, Duration.ofSeconds(1));
        assertFalse(future.isDone(), "Future should not be done before completion");

        awaiter.complete(KEY, event);

        Event received = future.get(); // shouldn't block long
        assertSame(event, received, "Future should complete with the event that was completed");
    }

    @Test
    void completeEarly_ThenAwait_ReturnsCompletedFutureImmediately() throws Exception {
        EventAwaiter<String, Event> awaiter = new EventAwaiter<>();
        Event event = mock(Event.class);

        // Event arrives before anyone awaits
        awaiter.complete(KEY, event);

        // Later, an await should return an already-completed future
        CompletableFuture<Event> future = awaiter.await(KEY, Duration.ofSeconds(1));
        assertTrue(future.isDone(), "Future should be completed immediately for early event");

        Event received = future.get();
        assertSame(event, received);
    }

    @Test
    void awaitWithTimeout_TimesOutIfNotCompleted() {
        EventAwaiter<String, Event> awaiter = new EventAwaiter<>();

        CompletableFuture<Event> future = awaiter.await(KEY, Duration.ofMillis(50));

        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> future.get(500, java.util.concurrent.TimeUnit.MILLISECONDS),
                "Future should complete exceptionally due to timeout"
        );
        assertInstanceOf(TimeoutException.class, ex.getCause(), "Cause should be a TimeoutException" );
    }

    @Test
    void completeExceptionally_PropagatesToAwaiter() {
        EventAwaiter<String, Event> awaiter = new EventAwaiter<>();
        RuntimeException failure = new RuntimeException("boom");

        CompletableFuture<Event> future = awaiter.await(KEY, Duration.ofSeconds(1));

        boolean signalled = awaiter.completeExceptionally(KEY, failure);
        assertTrue(signalled, "Should return true when a waiting future was completed exceptionally");

        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertSame(failure, ex.getCause(), "Future should carry the same exception instance");
    }

    @Test
    void completeExceptionally_NoAwaiter_ReturnsFalse() {
        EventAwaiter<String, Event> awaiter = new EventAwaiter<>();
        boolean signalled = awaiter.completeExceptionally(KEY, new RuntimeException("boom"));
        assertFalse(signalled, "Should return false when no future was awaiting");
    }

    @Test
    void multipleAwaitersForSameKey_AllCompleteOnSingleEvent() throws Exception {
        EventAwaiter<String, Event> awaiter = new EventAwaiter<>();
        Event event = mock(Event.class);

        CompletableFuture<Event> f1 = awaiter.await(KEY, Duration.ofSeconds(1));
        CompletableFuture<Event> f2 = awaiter.await(KEY, Duration.ofSeconds(1));

        awaiter.complete(KEY, event);

        assertSame(event, f1.get());
        assertSame(event, f2.get());
    }

    @Test
    void secondAwaitAfterCompletion_UsesNewFuture() throws Exception {
        EventAwaiter<String, Event> awaiter = new EventAwaiter<>();
        Event first = mock(Event.class);
        Event second = mock(Event.class);

        // First await/complete
        CompletableFuture<Event> f1 = awaiter.await(KEY, Duration.ofSeconds(1));
        awaiter.complete(KEY, first);
        assertSame(first, f1.get());

        // After completion, the internal pending future is removed.
        // A second await should NOT be completed yet until a new event is completed.
        CompletableFuture<Event> f2 = awaiter.await(KEY, Duration.ofSeconds(1));
        assertFalse(f2.isDone(), "Second await should not be done yet");

        awaiter.complete(KEY, second);
        assertSame(second, f2.get());
    }
}
