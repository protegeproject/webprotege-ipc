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
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;
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

    @Value("${webprotege.rabbitmq.prefetch-count:25}")
    private int prefetchCount;

    @Value("${webprotege.rabbitmq.connection-timeout:30000}")
    private int connectionTimeout;

    @Value("${webprotege.rabbitmq.heartbeat:60}")
    private int heartbeat;

    @Value("${webprotege.rabbitmq.connection-pool-size:20}")
    private int connectionPoolSize;

    @Value("${webprotege.rabbitmq.retry-max-attempts:3}")
    private int maxRetryAttempts;

    @Value("${webprotege.rabbitmq.retry-initial-interval:1000}")
    private long initialRetryInterval;

    @Value("${webprotege.rabbitmq.retry-multiplier:2.0}")
    private double retryMultiplier;

    @Value("${webprotege.rabbitmq.retry-max-interval:10000}")
    private long maxRetryInterval;

    @Value("${webprotege.rabbitmq.channel-cache-size:50}")
    private int channelCacheSize;

    @Value("${webprotege.rabbitmq.connection-cache-size:5}")
    private int connectionCacheSize;

    @Value("${webprotege.rabbitmq.channel-checkout-timeout:60000}")
    private int channelCheckoutTimeout;

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
    public SimpleRetryPolicy retryPolicy() {
        SimpleRetryPolicy policy = new SimpleRetryPolicy();
        policy.setMaxAttempts(maxRetryAttempts);
        logger.info("Configured retry policy with max attempts: {}", maxRetryAttempts);
        return policy;
    }

    @Bean
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
    public ExponentialBackOffPolicy backOffPolicy() {
        ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
        policy.setInitialInterval(initialRetryInterval);
        policy.setMultiplier(retryMultiplier);
        policy.setMaxInterval(maxRetryInterval);
        logger.info("Configured backoff policy with initial interval: {}ms, multiplier: {}, max interval: {}ms", 
                   initialRetryInterval, retryMultiplier, maxRetryInterval);
        return policy;
    }

    @Bean
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
    public RetryTemplate retryTemplate() {
        RetryTemplate template = new RetryTemplate();
        template.setRetryPolicy(retryPolicy());
        template.setBackOffPolicy(backOffPolicy());
        return template;
    }

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
       
       // Add retry template for resilience
       rabbitTemplate.setRetryTemplate(retryTemplate());
       
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
        container.setChannelTransacted(false);
        container.setConcurrency("25-35");  // Increased from "15-20"
        // Use auto acknowledgment to let Spring handle it
        container.setAcknowledgeMode(AcknowledgeMode.AUTO);
        
        return container;
    }

    @Bean
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
    public RabbitMqCommandHandlerWrapper rabbitMqCommandHandlerWrapper(@Value("${spring.application.name}") String applicationName){
       return new RabbitMqCommandHandlerWrapper<>(applicationName, handlers, objectMapper, authorizationStatusExecutor);
    }

    @Bean
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
    public SimpleMessageListenerContainer messageListenerContainers(RabbitMqCommandHandlerWrapper rabbitMqCommandHandlerWrapper) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setQueueNames(getCommandQueue());
        container.setConnectionFactory(connectionFactory);
        container.setChannelTransacted(false);
        container.setMessageListener(rabbitMqCommandHandlerWrapper);
        container.setConcurrency("25-35");  // Increased from "15-20"
        // Use auto acknowledgment to let Spring handle it
        container.setAcknowledgeMode(AcknowledgeMode.AUTO);
        
        return container;
    }

    @PostConstruct
    @ConditionalOnProperty(prefix = "webprotege.rabbitmq", name = "commands-subscribe", havingValue = "true", matchIfMissing = true)
    public void  createBindings() {
        try (Connection connection = connectionFactory.createConnection();
             Channel channel = connection.createChannel(false)) {
            channel.exchangeDeclare(COMMANDS_EXCHANGE, "direct", true);
            channel.queueDeclare(getCommandQueue(), true, false, false, null);
            channel.queueDeclare(getCommandResponseQueue(), true, false, false, null);
            channel.basicQos(prefetchCount);

            logger.info("Configuring RabbitMQ with prefetch count: {}, retry attempts: {}, connection pool: {}", 
                       prefetchCount, maxRetryAttempts, connectionPoolSize);

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
