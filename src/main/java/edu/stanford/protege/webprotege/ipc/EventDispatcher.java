package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.common.ProjectEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-04
 */
public class EventDispatcher {

    public static final String WEBPROTEGE_EVENTS_CHANNEL_NAME = "webprotege.events";

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper;

    public EventDispatcher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Dispatch the specified event.  The event will be dispatched to its own event-specific channel and
     * to the general {@code webprotege.events} channel.  The message containing the event that is dispatched
     * to the general channel contains a header specifying the event-specific channel name as a type for the event.
     *
     * @param event The event to be dispatched.
     */
    public void dispatchEvent(Event event) {
        try {
            var json = objectMapper.writeValueAsString(event);

            var channelSpecificMessage = MessageBuilder.withPayload(json)
                                        .setHeader(KafkaHeaders.TOPIC, event.getChannel());
            addProjectHeader(event, channelSpecificMessage);
            kafkaTemplate.send(channelSpecificMessage.build());

            var eventsChannelMessageBuilder = MessageBuilder.withPayload(json)
                                                     .setHeader(Headers.EVENT_TYPE, event.getChannel())
                                                     .setHeader(KafkaHeaders.TOPIC, WEBPROTEGE_EVENTS_CHANNEL_NAME);
            addProjectHeader(event, eventsChannelMessageBuilder);
            kafkaTemplate.send(eventsChannelMessageBuilder.build());
        } catch (JsonProcessingException e) {
            throw new EventDispatchException(e);
        }
    }

    private void addProjectHeader(Event event, MessageBuilder<String> channelSpecificMessage) {
        if (!(event instanceof ProjectEvent)) {
            return;
        }
        var projectId = ((ProjectEvent) event).projectId().value();
        channelSpecificMessage.setHeader(Headers.PROJECT_ID, projectId);
    }
}
