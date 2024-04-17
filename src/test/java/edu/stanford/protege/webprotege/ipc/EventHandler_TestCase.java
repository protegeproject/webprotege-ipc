package edu.stanford.protege.webprotege.ipc;


import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.common.ProjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-08
 */
@SpringBootTest(classes = WebProtegeIpcApplication.class)
@TestPropertySource(properties = "webprotege.rabbitmq.event-subscribe=true")
public class EventHandler_TestCase extends IntegrationTestsExtension {
    public static CountDownLatch countDownLatch;

    @Autowired
    private EventDispatcher eventDispatcher;

    private static final String EVENT_ID = UUID.randomUUID().toString();
    private final ProjectId projectId = ProjectId.generate();

    @SpyBean
    private static TestEventHandler testEventHandler;

    @Captor
    ArgumentCaptor<? extends Event> testHandlerCaptor;

    @Test
    void shouldInstantiateEventDispatcher() {
        assertThat(eventDispatcher).isNotNull();
    }
    @BeforeEach
    void setUp() {
        countDownLatch = new CountDownLatch(1);
    }
    @Test
    void shouldContainEventId() throws InterruptedException {
        eventDispatcher.dispatchEvent(new TestEvent(EVENT_ID, projectId.id()));
        assertThat(countDownLatch.await(60, TimeUnit.SECONDS)).isTrue();
        verify(testEventHandler).handleEvent((TestEvent) testHandlerCaptor.capture());
        var event = testHandlerCaptor.getValue();
        assertEquals(EVENT_ID, event.eventId().id());
    }

    @Test
    void shouldContainProjectId() throws InterruptedException {
        eventDispatcher.dispatchEvent(new TestEvent(EVENT_ID, projectId.id()));
        assertThat(countDownLatch.await(60, TimeUnit.SECONDS)).isTrue();
        verify(testEventHandler).handleEvent((TestEvent) testHandlerCaptor.capture());
        TestEvent event = (TestEvent) testHandlerCaptor.getValue();
        assertEquals(projectId.id(), event.projectId().value());
    }

/*    @Configuration
    public static class TestConfiguration {

        @Bean
        public EventHandler<TestEvent> getEventHandler(){
            return testEventHandler;
        }

    }*/
}
