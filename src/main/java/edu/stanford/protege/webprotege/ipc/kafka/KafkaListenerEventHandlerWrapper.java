package edu.stanford.protege.webprotege.ipc.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.ipc.EventHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-10-08
 */
public class KafkaListenerEventHandlerWrapper<E extends Event> {

    public static final String METHOD_NAME = "handleEvent";

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerCommandHandlerWrapper.class);

    private final ObjectMapper objectMapper;

    private final EventHandler<E> eventHandler;

    public KafkaListenerEventHandlerWrapper(ObjectMapper objectMapper,
                                            EventHandler<E> eventHandler) {
        this.objectMapper = objectMapper;
        this.eventHandler = eventHandler;
    }

    public void handleEvent(final ConsumerRecord<String, String> record) {
        var payload = record.value();
        var event = deserializeEvent(payload);

        if(event == null) {
            logger.error("Unable to parse event");
            return;
        }

        try {
            eventHandler.handleEvent(event);
        }
        catch (Exception e) {
            logger.info("An unhandled exception occurred while asking a listener to handle an event {} {}",
                        e.getClass().getSimpleName(),
                        e.getMessage());
        }
    }

    private E deserializeEvent(String request) {
        try {
            return objectMapper.readValue(request, eventHandler.getEventClass());
        } catch (JsonProcessingException e) {
            logger.error("Error while deserializing request", e);
            return null;
        }
    }
}
