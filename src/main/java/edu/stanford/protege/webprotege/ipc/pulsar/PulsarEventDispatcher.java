package edu.stanford.protege.webprotege.ipc.pulsar;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.common.EventId;
import edu.stanford.protege.webprotege.common.ProjectEvent;
import edu.stanford.protege.webprotege.ipc.EventDispatcher;
import edu.stanford.protege.webprotege.ipc.EventRecord;
import edu.stanford.protege.webprotege.ipc.GenericEventHandler;
import edu.stanford.protege.webprotege.ipc.Headers;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import static edu.stanford.protege.webprotege.ipc.Headers.*;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-03
 */
public class PulsarEventDispatcher implements EventDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(PulsarEventDispatcher.class);

    private final String applicationName;

    private final PulsarProducersManager producersManager;

    private final ObjectMapper objectMapper;

    private final String tenant;

    public PulsarEventDispatcher(@Value("${spring.application.name}") String applicationName,
                                 PulsarProducersManager producersManager,
                                 ObjectMapper objectMapper,
                                 @Value("${webprotege.pulsar.tenant}") String tenant) {
        this.applicationName = applicationName;
        this.producersManager = producersManager;
        this.objectMapper = objectMapper;
        this.tenant = tenant;
    }

    @Override
    public void dispatchEvent(Event event) {
        createProducerAndDispatchEvent(event);
    }

    private void createProducerAndDispatchEvent(Event event) {
        var eventTopicUrl = tenant + "/" + PulsarNamespaces.EVENTS + "/" + event.getChannel();
        var producer = producersManager.getProducer(eventTopicUrl);
        serializeAndDispatchEvent(event, producer);
        serializeAndDispatchEventRecord(event);
    }

    private void serializeAndDispatchEvent(Event event, Producer<byte[]> producer) {
        try {
            var value = objectMapper.writeValueAsBytes(event);
            var messageBuilder = producer.newMessage()
                    .value(value);
            getJsonTypeName(event).ifPresent(typeName -> messageBuilder.property(EVENT_TYPE, typeName));
            if(event instanceof ProjectEvent) {
                var projectId = ((ProjectEvent) event).projectId().value();
                messageBuilder.property(PROJECT_ID, projectId);
            }
            var messageId = messageBuilder.send();
            logger.info("Sent event message: {}", messageId);
        } catch (JsonProcessingException e) {
            logger.info("Could not serialize event: {}", e.getMessage(), e);
        } catch (PulsarClientException e) {
            logger.error("Could not send event message", e);
        }
    }

    private void serializeAndDispatchEventRecord(Event event) {
        try {
            var allEventsTopicUrl = tenant + "/" + PulsarNamespaces.EVENTS + "/" + GenericEventHandler.ALL_EVENTS_CHANNEL;
            var allEventsProducer = producersManager.getProducer(allEventsTopicUrl);
            var value = objectMapper.writeValueAsBytes(event);

            var projectId = event instanceof ProjectEvent ? ((ProjectEvent) event).projectId() : null;
            var timestamp = System.currentTimeMillis();
            var record = new EventRecord(event.eventId(), timestamp, event.getChannel(), value, projectId);
            var recordValue = objectMapper.writeValueAsBytes(record);
            var messageBuilder = allEventsProducer.newMessage()
                    .value(recordValue)
                    .property(EVENT_TYPE, event.getChannel());
            if(record.projectId() != null) {
                messageBuilder.property(PROJECT_ID, record.projectId().value());
            }
            var messageId = messageBuilder.send();
            logger.info("Sent event record message: {}", messageId);
        } catch (JsonProcessingException e) {
            logger.info("Could not serialize event: {}", e.getMessage(), e);
        } catch (PulsarClientException e) {
            logger.error("Could not send event message", e);
        }
    }

    private String getProducerName(Event event) {
        return applicationName + "--" + event.getChannel() + "--event-producer";
    }

    private Optional<String> getJsonTypeName(Event event) {
        var annotation = event.getClass().getAnnotation(JsonTypeName.class);
        return Optional.ofNullable(annotation).map(JsonTypeName::value);
    }
}
