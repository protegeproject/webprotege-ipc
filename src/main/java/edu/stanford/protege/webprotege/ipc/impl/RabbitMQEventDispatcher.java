package edu.stanford.protege.webprotege.ipc.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.common.ProjectEvent;
import edu.stanford.protege.webprotege.ipc.EventDispatcher;
import edu.stanford.protege.webprotege.ipc.ExecutionContext;
import edu.stanford.protege.webprotege.ipc.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Optional;

import static edu.stanford.protege.webprotege.ipc.Headers.*;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-03
 */
public class RabbitMQEventDispatcher implements EventDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQEventDispatcher.class);


    private final ObjectMapper objectMapper;

    private final RabbitTemplate eventRabbitTemplate;


    public RabbitMQEventDispatcher(ObjectMapper objectMapper,
                                   @Qualifier("eventRabbitTemplate") RabbitTemplate eventRabbitTemplate) {
        this.objectMapper = objectMapper;
        this.eventRabbitTemplate = eventRabbitTemplate;
    }

    @Override
    public void dispatchEvent(Event event, ExecutionContext executionContext) {
        try {
            var value = objectMapper.writeValueAsBytes(event);
            Message message = MessageBuilder.withBody(value).build();
            getJsonTypeName(event).ifPresent(typeName ->message.getMessageProperties().getHeaders().put(EVENT_TYPE, typeName));
            message.getMessageProperties().getHeaders().put(CHANNEL, event.getChannel());

            if(executionContext != null) {
                message.getMessageProperties().getHeaders().put(Headers.ACCESS_TOKEN, executionContext.jwt());
                message.getMessageProperties().getHeaders().put(Headers.USER_ID, executionContext.userId().id());
            }

            if(event instanceof ProjectEvent) {
                var projectId = ((ProjectEvent) event).projectId().value();
                message.getMessageProperties().getHeaders().put(PROJECT_ID, projectId);
            }
            eventRabbitTemplate.convertAndSend(RabbitMQEventsConfiguration.EVENT_EXCHANGE, "", message);
        } catch (JsonProcessingException | AmqpException e) {
            logger.info("Could not serialize event: {}", e.getMessage(), e);
        }
    }

    @Override
    public void dispatchEvent(Event event) {
      dispatchEvent(event, null);
    }
/*
    TODO remove this after everything regarding events is clear
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
    */
    private Optional<String> getJsonTypeName(Event event) {
        var annotation = event.getClass().getAnnotation(JsonTypeName.class);
        return Optional.ofNullable(annotation).map(JsonTypeName::value);
    }
}
