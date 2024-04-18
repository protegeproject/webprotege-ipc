package edu.stanford.protege.webprotege.ipc.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.ipc.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;

import java.io.IOException;
import java.util.List;

import static edu.stanford.protege.webprotege.ipc.Headers.CHANNEL;

public class RabbitMQEventHandlerWrapper<T extends Event> implements MessageListener {

    private final static Logger logger = LoggerFactory.getLogger(RabbitMQEventHandlerWrapper.class);

   private final List<EventHandler<? extends Event>> eventHandlers;

   private final ObjectMapper objectMapper;

    public RabbitMQEventHandlerWrapper(List<EventHandler<? extends Event>> eventHandlers, ObjectMapper objectMapper) {
        this.eventHandlers = eventHandlers;
        this.objectMapper = objectMapper;
    }

    @Override
    public void onMessage(Message message) {
        logger.info("Handling event with id {}", message.getMessageProperties().getMessageId());
        EventHandler eventHandler = eventHandlers.stream()
                .filter(handler -> {
                    String channel = String.valueOf(message.getMessageProperties().getHeaders().get(CHANNEL));
                    return channel.contains(handler.getChannelName());
                }).findFirst()
                .orElse(null);
        if(eventHandler != null) {
            try {
                T event = (T) objectMapper.readValue(message.getBody(), eventHandler.getEventClass());
                eventHandler.handleEvent(event);
            } catch (IOException e) {
                logger.error("Error when handling event "+ message.getMessageProperties().getMessageId(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
