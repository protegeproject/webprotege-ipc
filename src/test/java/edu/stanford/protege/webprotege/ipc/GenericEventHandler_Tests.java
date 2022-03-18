package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.common.EventId;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static edu.stanford.protege.webprotege.ipc.GenericEventHandler_Tests.TestEvent.CHANNEL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-11
 */
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class GenericEventHandler_Tests {


    protected static final String HANDLER_NAME = "GenericEventHandlerTestHandler";

    protected static final String TOPIC_URL = "persistent://webprotege-ipc-test-tenant/events/webprotege.events.all";

    private static String testEventId;


    private static CountDownLatch countDownLatch;

    @Autowired
    private EventDispatcher eventDispatcher;

    @Autowired
    private PulsarAdmin pulsarAdmin;

    @Value("${webprotege.pulsar.tenant}")
    private String tenant;

    @Value("${spring.application.name}")
    private String applicationName;


    @BeforeEach
    void setUp() throws PulsarAdminException {
        countDownLatch = new CountDownLatch(1);
        testEventId = UUID.randomUUID().toString();
    }

    @AfterEach
    void tearDown() throws PulsarAdminException {
    }

    @Test
    void shouldReceiveEvent() throws InterruptedException, PulsarAdminException {
        eventDispatcher.dispatchEvent(new TestEvent(testEventId));
        assertThat(countDownLatch.await(60, TimeUnit.SECONDS)).isTrue();
    }

    @JsonTypeName(CHANNEL)
    public static class TestEvent implements Event {

        private final EventId eventId = EventId.generate();

        private String id;

        protected static final String CHANNEL = "test.events.TestEvent";

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
            return eventId;
        }

        @Override
        public String getChannel() {
            return CHANNEL;
        }
    }

    @WebProtegeHandler
    public static class TestGenericEventHandler implements GenericEventHandler {

        @Autowired
        private ObjectMapper objectMapper;

        @Nonnull
        @Override
        public String getHandlerName() {
            return HANDLER_NAME;
        }

        @Override
        public void handlerSubscribed() {
            System.out.println("The handler has been subscribed");
        }

        @Override
        public void handleEventRecord(EventRecord eventRecord) {
            try {
                assertThat(eventRecord.eventType()).isEqualTo("test.events.TestEvent");
                byte [] payload = eventRecord.eventPayload();
                var deserializedTestEvent = objectMapper.readValue(payload, TestEvent.class);
                assertThat(deserializedTestEvent).isNotNull();
                assertThat(deserializedTestEvent.getId()).isEqualTo(testEventId);
                countDownLatch.countDown();
            } catch (IOException e) {
                fail("Failed reading payload", e);
            }
        }
    }

    @TestConfiguration
    public static class TestConf {

        @Bean
        TestGenericEventHandler testGenericEventHandler() {
            return new TestGenericEventHandler();
        }

    }
}
