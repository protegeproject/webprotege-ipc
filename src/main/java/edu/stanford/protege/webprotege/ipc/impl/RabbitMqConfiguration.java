package edu.stanford.protege.webprotege.ipc.impl;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.CommandExecutor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import edu.stanford.protege.webprotege.ipc.CommandHandler;
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
@ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
public class RabbitMqConfiguration {

    private final static Logger logger = LoggerFactory.getLogger(RabbitMqConfiguration.class);


    @Value("${webprotege.rabbitmq.responsequeue}")
    public String commandsResponseQueue;

    @Value("${webprotege.rabbitmq.requestqueue}")
    public String commandsQueue;

    @Value("${webprotege.rabbitmq.timeout}")
    public Long rabbitMqTimeout;


    public static final String COMMANDS_EXCHANGE = "webprotege-exchange";


    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired(required = false)
    private List<CommandHandler<? extends Request, ? extends Response>> handlers = new ArrayList<>();

    @Autowired
    private CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor;

    @Bean
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
    Queue msgQueue() {
        return new Queue(getCommandQueue(), true);
    }

    @Bean
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
    Queue replyQueue() {
        return new Queue(getCommandResponseQueue(), true);
    }

    @Bean
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
    DirectExchange exchange() {
        return new DirectExchange(COMMANDS_EXCHANGE, true, false);
    }



   @Bean(name = "rabbitTemplate")
   @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
   public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
       RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
       rabbitTemplate.setReplyTimeout(rabbitMqTimeout);
       rabbitTemplate.setExchange(COMMANDS_EXCHANGE);
       return rabbitTemplate;
   }

    @Bean(name = "asyncRabbitTemplate")
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
    public AsyncRabbitTemplate asyncRabbitTemplate(@Qualifier("rabbitTemplate") RabbitTemplate rabbitTemplate, SimpleMessageListenerContainer replyListenerContainer) {
        var asyncRabbitTemplate = new AsyncRabbitTemplate(rabbitTemplate,
                                                          replyListenerContainer,
                                                          getCommandResponseQueue());
        asyncRabbitTemplate.setReceiveTimeout(rabbitMqTimeout);
        return asyncRabbitTemplate;
    }



    @Bean
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
    public SimpleMessageListenerContainer replyListenerContainer(ConnectionFactory connectionFactory, Queue replyQueue) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueues(replyQueue);
        container.setConcurrency("15-20");
        return container;
    }

    @Bean
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
    public RabbitMqCommandHandlerWrapper rabbitMqCommandHandlerWrapper(){
       return new RabbitMqCommandHandlerWrapper<>(handlers, objectMapper, authorizationStatusExecutor);
    }

    @Bean
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
    public SimpleMessageListenerContainer messageListenerContainers() {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setQueueNames(getCommandQueue());
        container.setConnectionFactory(connectionFactory);
        container.setMessageListener(rabbitMqCommandHandlerWrapper());
        container.setConcurrency("15-20");
        return container;
    }

    @PostConstruct
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
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
            throw new RuntimeException("Error initialize bindings", e);
        }
    }

    private String getCommandQueue(){
        return commandsQueue;
    }

    private String getCommandResponseQueue() {
        return commandsResponseQueue;
    }

}
