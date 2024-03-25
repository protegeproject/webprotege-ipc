package edu.stanford.protege.webprotege.ipc.impl;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.CommandExecutor;
import org.springframework.context.ApplicationContext;
import edu.stanford.protege.webprotege.ipc.CommandHandler;
import edu.stanford.protege.webprotege.ipc.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Configuration
public class RabbitMqConfiguration {

    private final static Logger logger = LoggerFactory.getLogger(RabbitMqConfiguration.class);


    @Value("${webprotege.rabbitmq.responsequeue}")
    public String commandsResponseQueue;

    @Value("${webprotege.rabbitmq.requestqueue}")
    public String commandsQueue;

    @Value("${webprotege.rabbitmq.timeout}")
    public Long rabbitMqTimeout;


    public static final String COMMANDS_EXCHANGE = "webprotege-exchange";

    public static final String EVENT_EXCHANGE = "webprotege-event-exchange";

    public static final String EVENT_QUEUE = "webprotege-event-queue";

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired(required = false)
    private List<EventHandler<? extends Event>> eventHandlers = new ArrayList<>();


    @Autowired(required = false)
    private List<CommandHandler<? extends Request, ? extends Response>> handlers = new ArrayList<>();


    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor;

    @Bean
    Queue msgQueue() {
        return new Queue(getCommandQueue(), true);
    }

    @Bean
    Queue replyQueue() {
        return new Queue(getCommandResponseQueue(), true);
    }

    @Bean
    public Queue eventsQueue() {
        return new Queue(EVENT_QUEUE, true);
    }

    @Bean
    DirectExchange exchange() {
        return new DirectExchange(COMMANDS_EXCHANGE, true, false);
    }

    @Bean
    FanoutExchange eventExchange() {
        return new FanoutExchange(EVENT_EXCHANGE, true, false);
    }

   @Bean(name = "rabbitTemplate")
   public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
       RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
       rabbitTemplate.setReplyTimeout(rabbitMqTimeout);
       rabbitTemplate.setExchange(COMMANDS_EXCHANGE);
       return rabbitTemplate;
   }

    @Bean(name = "asyncRabbitTemplate")
    public AsyncRabbitTemplate asyncRabbitTemplate(@Qualifier("rabbitTemplate") RabbitTemplate rabbitTemplate, SimpleMessageListenerContainer replyListenerContainer) {
        return new AsyncRabbitTemplate(rabbitTemplate, replyListenerContainer, getCommandResponseQueue());
    }

    @Bean(name = "eventRabbitTemplate")
    public RabbitTemplate eventRabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setReplyTimeout(rabbitMqTimeout);
        rabbitTemplate.setExchange(EVENT_EXCHANGE);
        rabbitTemplate.setUseDirectReplyToContainer(false);
        return rabbitTemplate;
    }

    @Bean
    public SimpleMessageListenerContainer eventsListenerContainer(ConnectionFactory connectionFactory, Queue eventsQueue, @Qualifier("eventRabbitTemplate") RabbitTemplate eventRabbitTemplate) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(EVENT_QUEUE);
        container.setMessageListener(new RabbitMQEventHandlerWrapper(eventHandlers, objectMapper));
        return container;
    }

    @Bean
    public SimpleMessageListenerContainer replyListenerContainer(ConnectionFactory connectionFactory, Queue replyQueue) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueues(replyQueue);
        return container;
    }

    @Bean
    public RabbitMqCommandHandlerWrapper rabbitMqCommandHandlerWrapper(){
       return new RabbitMqCommandHandlerWrapper<>(handlers, objectMapper, authorizationStatusExecutor);
    }

    @Bean
    public SimpleMessageListenerContainer messageListenerContainers() {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setQueueNames(getCommandQueue());
        container.setConnectionFactory(connectionFactory);
        container.setMessageListener(rabbitMqCommandHandlerWrapper());
        return container;
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

    @PostConstruct
    public void  createBindings() {
        try (Connection connection = connectionFactory.createConnection();
             Channel channel = connection.createChannel(true)) {
            channel.exchangeDeclare(COMMANDS_EXCHANGE, "direct", true);
            channel.queueDeclare(getCommandQueue(), true, false, false, null);
            channel.queueDeclare(getCommandResponseQueue(), true, false, false, null);
            channel.basicQos(1);

            for (CommandHandler handler : handlers) {
                logger.info("Declaring binding queue {} to exchange {} with key {}", getCommandQueue(), COMMANDS_EXCHANGE, handler.getChannelName());
                channel.queueBind(getCommandQueue(), COMMANDS_EXCHANGE, handler.getChannelName());
            }
            channel.queueBind(getCommandResponseQueue(), COMMANDS_EXCHANGE, getCommandResponseQueue());

        } catch (IOException |TimeoutException e) {
            logger.error("Error initialize bindings", e);
        }
    }

    private String getCommandQueue(){
        return commandsQueue;
    }

    private String getCommandResponseQueue() {
        return commandsResponseQueue;
    }

}
