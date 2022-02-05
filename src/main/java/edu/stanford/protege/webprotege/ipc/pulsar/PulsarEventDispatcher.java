package edu.stanford.protege.webprotege.ipc.pulsar;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.common.ProjectEvent;
import edu.stanford.protege.webprotege.ipc.EventDispatcher;
import edu.stanford.protege.webprotege.ipc.Headers;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

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

    public PulsarEventDispatcher(@Value("${spring.application.name}") String applicationName,
                                 PulsarProducersManager producersManager,
                                 ObjectMapper objectMapper) {
        this.applicationName = applicationName;
        this.producersManager = producersManager;
        this.objectMapper = objectMapper;
    }

    @Override
    public void dispatchEvent(Event event) {
        createProducerAndDispatchEvent(event);
    }

    private void createProducerAndDispatchEvent(Event event) {
        var eventTopicUrl = TopicUrl.getEventTopicUrl(event.getChannel());
        var producer = producersManager.getProducer(eventTopicUrl, producerBuilder -> {
            producerBuilder.producerName(getProducerName(event));
        });
        serializeAndDispatchEvent(event, producer);
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
            messageBuilder.sendAsync();
        } catch (JsonProcessingException e) {
            logger.info("Could not serialize event: {}", e.getMessage(), e);
        }
    }

    private String getProducerName(Event event) {
        return applicationName + "-" + event.getChannel() + "-producer";
    }

    private Optional<String> getJsonTypeName(Event event) {
        var annotation = event.getClass().getAnnotation(JsonTypeName.class);
        return Optional.ofNullable(annotation).map(JsonTypeName::value);
    }
}
