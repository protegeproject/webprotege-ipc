package edu.stanford.protege.webprotege.ipc.impl;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.common.UserId;
import edu.stanford.protege.webprotege.ipc.EventHandler;
import edu.stanford.protege.webprotege.ipc.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import edu.stanford.protege.webprotege.ipc.util.CorrelationMDCUtil;

import java.io.IOException;
import java.util.List;

import static edu.stanford.protege.webprotege.ipc.Headers.*;

public class RabbitMQEventHandlerWrapper<T extends Event> implements ChannelAwareMessageListener {

    private final static Logger logger = LoggerFactory.getLogger(RabbitMQEventHandlerWrapper.class);

   private final List<EventHandler<? extends Event>> eventHandlers;

   private final ObjectMapper objectMapper;

    public RabbitMQEventHandlerWrapper(List<EventHandler<? extends Event>> eventHandlers, ObjectMapper objectMapper) {
        this.eventHandlers = eventHandlers;
        this.objectMapper = objectMapper;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws Exception {
        String correlationId = message.getMessageProperties().getCorrelationId();
        try {
            // Set correlation ID in MDC
            CorrelationMDCUtil.setCorrelationId(correlationId);
            
            EventHandler eventHandler = eventHandlers.stream()
                    .filter(handler -> {
                        String channgelString = String.valueOf(message.getMessageProperties().getHeaders().get(CHANNEL));
                        return channgelString.contains(handler.getChannelName());
                    }).findFirst()
                    .orElse(null);
            if(eventHandler != null) {
                try {
                    T event = (T) objectMapper.readValue(message.getBody(), eventHandler.getEventClass());
                    var accessToken = String.valueOf(message.getMessageProperties().getHeaders().get(ACCESS_TOKEN));
                    var userId = (String) message.getMessageProperties().getHeaders().get(USER_ID);
                    if(accessToken != null && !accessToken.isEmpty() && !"null".equalsIgnoreCase(accessToken)){
                        ExecutionContext executionContext = new ExecutionContext(UserId.valueOf(userId), accessToken, correlationId);
                        try {
                            eventHandler.handleEvent(event, executionContext);

                        } catch (EventHandlerMethodNotImplemented e){
                            eventHandler.handleEvent(event);
                        }
                    } else {
                        eventHandler.handleEvent(event);
                    }

                } catch (DatabindException | StreamReadException e) {
                    logger.error("Could not parse event. Event body: {}. Error: {}", message.getBody(), e.getMessage(), e);
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
                } catch (IOException e) {
                    logger.error("Error when handling event "+ message.getMessageProperties().getMessageId(), e);
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
                }
            }
        } finally {
            // Clear correlation ID from MDC
            CorrelationMDCUtil.clearCorrelationId();
        }
    }
}
