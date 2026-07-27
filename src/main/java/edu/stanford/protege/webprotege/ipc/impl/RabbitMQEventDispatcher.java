package edu.stanford.protege.webprotege.ipc.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.common.ProjectEvent;
import edu.stanford.protege.webprotege.ipc.EventDispatchException;
import edu.stanford.protege.webprotege.ipc.EventDispatcher;
import edu.stanford.protege.webprotege.ipc.ExecutionContext;
import edu.stanford.protege.webprotege.ipc.Headers;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.util.Optional;

import static edu.stanford.protege.webprotege.ipc.Headers.*;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-03
 */
public class RabbitMQEventDispatcher implements EventDispatcher {

    private final String applicationName;

    private final ObjectMapper objectMapper;

    private final RabbitTemplate eventRabbitTemplate;

    private final long confirmTimeout;


    public RabbitMQEventDispatcher(@Value("${spring.application.name}") String applicationName, ObjectMapper objectMapper,
                                   @Qualifier("eventRabbitTemplate") RabbitTemplate eventRabbitTemplate,
                                   @Value("${webprotege.rabbitmq.timeout}") long confirmTimeout) {
        this.applicationName = applicationName;
        this.objectMapper = objectMapper;
        this.eventRabbitTemplate = eventRabbitTemplate;
        this.confirmTimeout = confirmTimeout;
    }

    @Override
    public void dispatchEvent(Event event, ExecutionContext executionContext) {
        Message message = createMessage(event, executionContext);
        eventRabbitTemplate.invoke(operations -> {
            operations.convertAndSend(RabbitMQEventsConfiguration.EVENT_EXCHANGE, "", message);
            operations.waitForConfirmsOrDie(confirmTimeout);
            return null;
        });
    }

    @Override
    public void dispatchEvent(Event event) {
      dispatchEvent(event, null);
    }

    private Message createMessage(Event event, ExecutionContext executionContext) {
        try {
            var value = objectMapper.writeValueAsBytes(event);
            Message message = MessageBuilder.withBody(value).build();
            getJsonTypeName(event).ifPresent(typeName -> message.getMessageProperties().getHeaders().put(EVENT_TYPE, typeName));
            message.getMessageProperties().getHeaders().put(CHANNEL, event.getChannel());

            if (executionContext != null) {
                message.getMessageProperties().getHeaders().put(Headers.ACCESS_TOKEN, executionContext.jwt());
                message.getMessageProperties().getHeaders().put(Headers.USER_ID, executionContext.userId().id());
            }

            message.getMessageProperties().setHeader(SERVICE_NAME, applicationName);

            if (event instanceof ProjectEvent) {
                var projectId = ((ProjectEvent) event).projectId().value();
                message.getMessageProperties().getHeaders().put(PROJECT_ID, projectId);
            }
            return message;
        } catch (JsonProcessingException e) {
            throw new EventDispatchException(e);
        }
    }

    private Optional<String> getJsonTypeName(Event event) {
        var annotation = event.getClass().getAnnotation(JsonTypeName.class);
        return Optional.ofNullable(annotation).map(JsonTypeName::value);
    }
}
