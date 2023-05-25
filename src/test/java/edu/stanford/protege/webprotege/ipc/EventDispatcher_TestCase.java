package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.EventId;
import edu.stanford.protege.webprotege.common.ProjectEvent;
import edu.stanford.protege.webprotege.common.ProjectId;
import edu.stanford.protege.webprotege.ipc.pulsar.PulsarNamespaces;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-04
 */
@SpringBootTest
@ExtendWith(PulsarTestExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class EventDispatcher_TestCase {

    public static final String THE_EVENT_ID = "TheEventId";

    @Autowired
    private EventDispatcher eventDispatcher;

    @Autowired
    private PulsarClient pulsarClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${webprotege.pulsar.tenant}")
    private String tenant;

    private final ProjectId projectId = ProjectId.generate();

    private Consumer<byte[]> consumer;



    @BeforeEach
    void setUp() throws Exception {
        consumer = pulsarClient.newConsumer()
                               .topic(tenant + "/" + PulsarNamespaces.EVENTS + "/TestEventChannel")
                .subscriptionType(SubscriptionType.Shared)
                               .subscriptionName("test-consumer-" + UUID.randomUUID())
                               .subscribe();
        var event = new TestEvent(EventId.generate(), THE_EVENT_ID, projectId);
        eventDispatcher.dispatchEvent(event);
    }

    @AfterEach
    void tearDown() throws PulsarClientException, PulsarAdminException {
        consumer.unsubscribe();
    }

    @Test
    void shouldInstantiateEventDispatcher() {
        assertThat(eventDispatcher).isNotNull();
    }

    @Test
    void shouldContainEventTypeHeader() throws IOException {
        var message = consumer.receive();
        assertThat(message.getProperty("webprotege_eventType")).isEqualTo("TestEventType");
    }

    @Test
    void shouldContainProjectIdHeader() throws PulsarClientException {
        var message = consumer.receive();
        assertThat(message.getProperty("webprotege_projectId")).isEqualTo(projectId.value());
    }

    @Test
    void shouldContainBodyWithJsonRepresentation() throws IOException {
        var message = consumer.receive();
        var object = objectMapper.readValue(new String(message.getValue()), new TypeReference<Map<String, Object>>() {});
        assertThat(object).containsEntry("id", THE_EVENT_ID);
        assertThat(object).containsEntry("projectId", projectId.value());
    }

    @JsonTypeName("TestEventType")
    private static record TestEvent(EventId eventId,
                                    String id,
                                    ProjectId projectId) implements ProjectEvent {

        @Override
        public String getChannel() {
            return "TestEventChannel";
        }
    }
}
