package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.common.EventId;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-08
 */
@SpringBootTest
@ExtendWith(PulsarTestExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class EventHandler_TestCase {


    @Autowired
    PulsarClient pulsarClient;

    @Autowired
    PulsarAdmin pulsarAdmin;

    @Autowired
    private EventDispatcher eventDispatcher;

    private static final String EVENT_ID = UUID.randomUUID().toString();

    private static CountDownLatch countDownLatch;


    @BeforeEach
    void setUp() {
        countDownLatch = new CountDownLatch(1);
    }

    @AfterEach
    void tearDown() throws PulsarAdminException {

    }

    @Test
    void shouldHandleEvent() throws InterruptedException {
        eventDispatcher.dispatchEvent(new TestEvent(EVENT_ID));
        assertThat(countDownLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @WebProtegeHandler
    private static class TestEventHandler implements EventHandler<TestEvent> {

        @Nonnull
        @Override
        public String getChannelName() {
            return TestEvent.CHANNEL;
        }

        @Nonnull
        @Override
        public String getHandlerName() {
            return "TheTestEventHandler";
        }

        @Override
        public Class<TestEvent> getEventClass() {
            return TestEvent.class;
        }

        @Override
        public void handleEvent(TestEvent event) {
            assertThat(event.getId()).isEqualTo(EVENT_ID);
            countDownLatch.countDown();
        }
    }


    @JsonTypeName("TestEvent")
    private static class TestEvent implements Event {

        private static final String CHANNEL = "the.test.event.channel";

        private final String id;

        private EventId eventId;

        @JsonCreator
        public TestEvent(@JsonProperty("id") String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        @Nonnull
        @Override
        public EventId eventId() {
            eventId = EventId.generate();
            return eventId;
        }

        @Override
        public String getChannel() {
            return CHANNEL;
        }
    }

    @TestConfiguration
    public static class TestConf {

        @Bean
        TestEventHandler testEventHandler() {
            return new TestEventHandler();
        }
    }
}
