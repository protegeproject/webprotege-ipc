package edu.stanford.protege.webprotege.ipc.impl;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.CommandExecutor;
import edu.stanford.protege.webprotege.ipc.CommandHandler;
import edu.stanford.protege.webprotege.ipc.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Configuration
public class RabbitMqConfiguration {

    private final static Logger logger = LoggerFactory.getLogger(RabbitMqConfiguration.class);

    @Value("${webprotege.rabbitmq.responsequeue}")
    public String COMMANDS_RESPONSE_QUEUE;
    @Value("${webprotege.rabbitmq.requestqueue}")
    public String COMMANDS_QUEUE;

    public static final String COMMANDS_EXCHANGE = "webprotege-exchange";

    public static final String EVENT_EXCHANGE = "webprotege-event-exchange";

    public static final String EVENT_QUEUE = "webprotege-event-queue";

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired(required = false)
    private List<CommandHandler<? extends Request, ? extends Response>> commandHandlers = new ArrayList<>();

    @Autowired(required = false)
    private List<EventHandler<? extends Event>> eventHandlers = new ArrayList<>();

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    @Lazy
    CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor;

    @Bean
    Queue msgQueue() {
        return new Queue(COMMANDS_QUEUE, true);
    }

    @Bean
    Queue replyQueue() {
        return new Queue(COMMANDS_RESPONSE_QUEUE, true);
    }

    @Bean
    public Queue eventsQueue() {
        return new Queue(EVENT_QUEUE, true);
    }

    @Bean
    TopicExchange exchange() {
        return new TopicExchange(COMMANDS_EXCHANGE, true, false);
    }

    @Bean
    FanoutExchange eventExchange() {
        return new FanoutExchange(EVENT_EXCHANGE, true, false);
    }



    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setReplyAddress(COMMANDS_RESPONSE_QUEUE);
        rabbitTemplate.setReplyTimeout(60000);
        rabbitTemplate.setExchange(COMMANDS_EXCHANGE);
        rabbitTemplate.setUseDirectReplyToContainer(false);
        return rabbitTemplate;
    }

    @Bean
    public RabbitTemplate eventRabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setReplyTimeout(60000);
        rabbitTemplate.setExchange(EVENT_EXCHANGE);
        rabbitTemplate.setUseDirectReplyToContainer(false);
        return rabbitTemplate;
    }

    @Bean
    public SimpleMessageListenerContainer eventsListenerContainer(ConnectionFactory connectionFactory, Queue eventsQueue, RabbitTemplate eventRabbitTemplate) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(EVENT_QUEUE);
        container.setMessageListener(new RabbitMQEventHandlerWrapper(eventHandlers, objectMapper));
        return container;
    }

    @Bean
    public SimpleMessageListenerContainer replyListenerContainer(ConnectionFactory connectionFactory, Queue replyQueue, RabbitTemplate rabbitTemplate) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueues(replyQueue);
        container.setMessageListener(rabbitTemplate);
        return container;
    }

    @Bean
    public SimpleMessageListenerContainer messageListenerContainers(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setQueueNames(COMMANDS_QUEUE);
        container.setConnectionFactory(connectionFactory);
        container.setMessageListener(new RabbitMqCommandHandlerWrapper<>(commandHandlers, objectMapper, authorizationStatusExecutor));
        return container;
    }

    @Bean
    public List<Binding> bindings(TopicExchange topicExchange, Queue msgQueue, Queue replyQueue) {

        var response = new ArrayList<Binding>();
        try (Connection connection = connectionFactory.createConnection();
             Channel channel = connection.createChannel(true)) {
            channel.exchangeDeclare(COMMANDS_EXCHANGE, "topic", true);
            channel.queueDeclare(COMMANDS_QUEUE, true, false, false, null);
            channel.queueDeclare(COMMANDS_RESPONSE_QUEUE, true, false, false, null);
            channel.basicQos(1);

            for (CommandHandler handler : commandHandlers) {
                logger.info("Declaring binding queue {} to exchange {} with key {}", COMMANDS_QUEUE, COMMANDS_EXCHANGE, handler.getChannelName());
                channel.queueBind(COMMANDS_QUEUE, COMMANDS_EXCHANGE, handler.getChannelName());
                response.add(BindingBuilder.bind(msgQueue).to(topicExchange).with(handler.getChannelName()));
            }
            channel.queueBind(COMMANDS_RESPONSE_QUEUE, COMMANDS_EXCHANGE, COMMANDS_RESPONSE_QUEUE);

            response.add(BindingBuilder.bind(replyQueue).to(topicExchange).with(replyQueue.getName()));
            channel.close();
            connection.close();
            return response;

        } catch (Exception e) {
            logger.error("Error initialize bindings", e);
        }
        return response;
    }

    @Bean
    public List<Binding> eventsBindings(FanoutExchange fanoutExchange, Queue eventsQueue) {
        var response = new ArrayList<Binding>();

        try (Connection connection = connectionFactory.createConnection();
             Channel channel = connection.createChannel(true)) {
            channel.exchangeDeclare(EVENT_EXCHANGE, "fanout", true);
            channel.queueDeclare(EVENT_QUEUE, true, false, false, null);

            for(EventHandler eventHandler : eventHandlers) {
                channel.queueBind(eventsQueue.getName(), fanoutExchange.getName(), eventHandler.getChannelName());
                response.add(BindingBuilder.bind(eventsQueue).to(fanoutExchange));
            }
        } catch (IOException | TimeoutException e) {
            logger.error("Error initialize bindings", e);
        }


        return response;
    }


}
