package edu.stanford.protege.webprotege.ipc.impl;


import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.ipc.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class RabbitMQEventsConfiguration {
    private final static Logger logger = LoggerFactory.getLogger(RabbitMQEventsConfiguration.class);

    @Value("${webprotege.rabbitmq.timeout}")
    public Long rabbitMqTimeout;

    @Value("${webprotege.rabbitmq.eventsqueue:EVENTS_QUEUE}")
    public String eventsQueue;


    public static final String EVENT_EXCHANGE = "webprotege-event-exchange";


    @Autowired(required = false)
    private List<EventHandler<? extends Event>> eventHandlers = new ArrayList<>();

    @Autowired
    private ObjectMapper objectMapper;

    @Bean
    public Queue eventsQueue() {
        return new Queue(eventsQueue, true);
    }


    @Bean
    FanoutExchange eventExchange() {
        return new FanoutExchange(EVENT_EXCHANGE, true, false);
    }


    @Bean(name = "eventRabbitTemplate")
    public RabbitTemplate eventRabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setReplyTimeout(rabbitMqTimeout);
        rabbitTemplate.setExchange(EVENT_EXCHANGE);
        return rabbitTemplate;
    }

    @Bean
    @ConditionalOnProperty(havingValue = "true", prefix = "webprotege.rabbitmq", name = "event-subscribe")
    public SimpleMessageListenerContainer eventsListenerContainer(ConnectionFactory connectionFactory) {
        logger.info("[RabbitMQEventConfiguration] Listening to event queue {}", eventsQueue);
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(eventsQueue);
        container.setMessageListener(new RabbitMQEventHandlerWrapper(eventHandlers, objectMapper));
        return container;
    }

    @Bean
    @ConditionalOnProperty(havingValue = "true", prefix = "webprotege.rabbitmq", name = "event-subscribe")
    public Binding binding(Queue eventsQueue, FanoutExchange fanoutExchange) {
        logger.info("[RabbitMQEventConfiguration] Binding to event queue {}", eventsQueue);

        return BindingBuilder.bind(eventsQueue).to(fanoutExchange);
    }


}
