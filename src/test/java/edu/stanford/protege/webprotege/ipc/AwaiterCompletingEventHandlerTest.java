package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Event;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link AwaiterCompletingEventHandler}.
 */
class AwaiterCompletingEventHandlerTest {

    private static final String CHANNEL = "test-channel";

    private static final String HANDLER = "test-handler";

    @Test
    void shouldThrowNpeWhenEventClassIsNull() {
        EventAwaiter<String, MyEvent> awaiter = new EventAwaiter<>();
        Function<MyEvent, String> extractor = e -> "k";
        assertThrows(NullPointerException.class,
                () -> new TestHandler(null, awaiter, extractor));
    }

    @Test
    void shouldThrowNpeWhenAwaiterIsNull() {
        Function<MyEvent, String> extractor = e -> "k";
        assertThrows(NullPointerException.class,
                () -> new TestHandler(MyEvent.class, null, extractor));
    }

    @Test
    void shouldThrowNpeWhenExtractorIsNull() {
        EventAwaiter<String, MyEvent> awaiter = new EventAwaiter<>();
        assertThrows(NullPointerException.class,
                () -> new TestHandler(MyEvent.class, awaiter, null));
    }

    @Test
    void shouldReturnProvidedEventClass() {
        EventAwaiter<String, MyEvent> awaiter = new EventAwaiter<>();
        TestHandler handler = new TestHandler(MyEvent.class, awaiter, e -> "k" );
        assertSame(MyEvent.class, handler.getEventClass());
    }

    @Test
    void shouldReturnOverriddenIdentityValues() {
        EventAwaiter<String, MyEvent> awaiter = new EventAwaiter<>();
        TestHandler handler = new TestHandler(MyEvent.class, awaiter, e -> "k" );
        assertEquals(CHANNEL, handler.getChannelName());
        assertEquals(HANDLER, handler.getHandlerName());
    }

    @Test
    void shouldCompleteAwaiterWithExtractedKeyWhenHandlingEvent() throws Exception {
        EventAwaiter<String, MyEvent> awaiter = new EventAwaiter<>();
        Function<MyEvent, String> extractor = e -> "event-key-123";
        TestHandler handler = new TestHandler(MyEvent.class, awaiter, extractor);

        MyEvent event = mock(MyEvent.class);

        var future = awaiter.await("event-key-123" , java.time.Duration.ofSeconds(1));
        handler.handleEvent(event);

        assertSame(event, future.get());
    }

    @Test
    void shouldDelegateContextAwareHandleEventToSimpleVariant() throws Exception {
        EventAwaiter<String, MyEvent> awaiter = new EventAwaiter<>();
        Function<MyEvent, String> extractor = e -> "ctx-key";
        TestHandler handler = new TestHandler(MyEvent.class, awaiter, extractor);

        MyEvent event = mock(MyEvent.class);
        ExecutionContext ctx = new ExecutionContext();

        var future = awaiter.await("ctx-key" , java.time.Duration.ofSeconds(1));
        handler.handleEvent(event, ctx);

        assertSame(event, future.get());
    }

    // Concrete event type for testing
    interface MyEvent extends Event {

    }

    /**
     * Minimal concrete subclass for testing with fixed identity.
     */
    static class TestHandler extends AwaiterCompletingEventHandler<String, MyEvent> {

        TestHandler(@Nonnull Class<MyEvent> eventClass,
                    @Nonnull EventAwaiter<String, MyEvent> awaiter,
                    @Nonnull Function<MyEvent, String> keyExtractor) {
            super(eventClass, awaiter, keyExtractor);
        }

        @Nonnull
        @Override
        public String getChannelName() {
            return CHANNEL;
        }

        @Nonnull
        @Override
        public String getHandlerName() {
            return HANDLER;
        }
    }
}
