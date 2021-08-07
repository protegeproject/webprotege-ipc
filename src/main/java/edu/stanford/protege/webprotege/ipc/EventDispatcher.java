package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-04
 */
public class EventDispatcher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper;

    public EventDispatcher(KafkaTemplate<String, String> kafkaTemplate,
                           ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void dispatchEvent(Event event) {
        try {
            var json = objectMapper.writeValueAsString(event);
            var message = MessageBuilder.withPayload(json)
                                        .setHeader(KafkaHeaders.TOPIC, event.getChannelName())
                                        .build();
            kafkaTemplate.send(message);
        } catch (JsonProcessingException e) {
            throw new EventDispatchException(e);
        }
    }
}
