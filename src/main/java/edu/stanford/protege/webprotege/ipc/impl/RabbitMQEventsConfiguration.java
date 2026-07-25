package edu.stanford.protege.webprotege.ipc.impl;


import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.ipc.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class RabbitMQEventsConfiguration implements DisposableBean {
    private final static Logger logger = LoggerFactory.getLogger(RabbitMQEventsConfiguration.class);

    @Value("${webprotege.rabbitmq.timeout}")
    public Long rabbitMqTimeout;

    @Value("${webprotege.rabbitmq.eventsqueue:EVENTS_QUEUE}")
    public String eventsQueue;

    /**
     * When {@code true}, the events queue becomes per service <em>instance</em> instead of
     * per service: a non-durable, exclusive, auto-delete queue with a uniquely generated name
     * (the configured {@link #eventsQueue} value as a prefix), so every instance binds its own
     * queue to the fanout exchange and therefore receives <em>every</em> event. Needed by
     * horizontally-scaled consumers that pin long-lived streams to a single instance (e.g. the
     * SSE gateway). Defaults to {@code false}, which keeps the historical competing-consumer
     * semantics: one shared durable queue across all instances of a service, each event handled
     * once. Do not flip the default — other services (e.g. backend-service) rely on it.
     */
    @Value("${webprotege.rabbitmq.event-queue-per-instance:false}")
    public boolean eventQueuePerInstance;


    public static final String EVENT_EXCHANGE = "webprotege-event-exchange";


    @Autowired(required = false)
    private List<EventHandler<? extends Event>> eventHandlers = new ArrayList<>();

    @Autowired
    private ObjectMapper objectMapper;

    private CachingConnectionFactory eventConnectionFactory;

    @Bean
    public Queue eventsQueue() {
        if (eventQueuePerInstance) {
            // AnonymousQueue is non-durable, exclusive and auto-delete, with a generated name.
            // Exclusive + auto-delete means a scaled-down instance's queue vanishes with it
            // instead of lingering, unbound-but-durable, and growing forever.
            return new AnonymousQueue(new Base64UrlNamingStrategy(eventsQueue + "-"));
        }
        return new Queue(eventsQueue, true);
    }


    @Bean
    FanoutExchange eventExchange() {
        return new FanoutExchange(EVENT_EXCHANGE, true, false);
    }


    @Bean(name = "eventRabbitTemplate")
    public RabbitTemplate eventRabbitTemplate(CachingConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(eventConnectionFactory(connectionFactory));
        rabbitTemplate.setReplyTimeout(rabbitMqTimeout);
        rabbitTemplate.setExchange(EVENT_EXCHANGE);
        return rabbitTemplate;
    }

    /**
     * A dedicated connection factory for publishing events, with publisher confirms enabled.
     * Confirms are scoped to the events template so that a normal return from
     * {@code dispatchEvent} means the broker has accepted the message; the command/RPC
     * templates keep using the auto-configured shared connection factory unchanged.
     * This is deliberately not exposed as a bean: an extra {@link ConnectionFactory} bean
     * would suppress Spring Boot's auto-configured connection factory (its
     * {@code @ConditionalOnMissingBean(ConnectionFactory.class)} backs off), so it is built
     * from the shared factory's underlying connection settings and its lifecycle is managed
     * here via {@link #destroy()}.
     */
    private CachingConnectionFactory eventConnectionFactory(CachingConnectionFactory connectionFactory) {
        if (eventConnectionFactory == null) {
            eventConnectionFactory = new CachingConnectionFactory(connectionFactory.getRabbitConnectionFactory());
            eventConnectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.SIMPLE);
        }
        return eventConnectionFactory;
    }

    @Override
    public void destroy() {
        if (eventConnectionFactory != null) {
            eventConnectionFactory.destroy();
        }
    }

    @Bean
    @ConditionalOnProperty(havingValue = "true", prefix = "webprotege.rabbitmq", name = "event-subscribe")
    public SimpleMessageListenerContainer eventsListenerContainer(ConnectionFactory connectionFactory, Queue eventsQueue) {
        logger.info("[RabbitMQEventConfiguration] Listening to event queue {}", eventsQueue.getActualName());
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        // Follow the queue object rather than a fixed name: in per-instance mode the actual
        // queue name is generated, so setQueueNames(eventsQueue) would subscribe to a queue
        // that never exists. The auto-configured RabbitAdmin re-declares the queue bean on
        // reconnect, recreating the exclusive queue after a broker outage.
        container.setQueues(eventsQueue);
        container.setMessageListener(new RabbitMQEventHandlerWrapper(eventHandlers, objectMapper));
        return container;
    }

    @Bean
    @ConditionalOnProperty(havingValue = "true", prefix = "webprotege.rabbitmq", name = "event-subscribe")
    public Binding binding(Queue eventsQueue, FanoutExchange fanoutExchange) {
        logger.info("[RabbitMQEventConfiguration] Binding to event queue {}", eventsQueue.getActualName());

        return BindingBuilder.bind(eventsQueue).to(fanoutExchange);
    }


}
