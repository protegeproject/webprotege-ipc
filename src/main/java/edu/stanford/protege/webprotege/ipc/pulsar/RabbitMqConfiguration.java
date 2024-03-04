package edu.stanford.protege.webprotege.ipc.pulsar;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.CommandExecutor;
import edu.stanford.protege.webprotege.ipc.CommandHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class RabbitMqConfiguration {

    private final static Logger logger = LoggerFactory.getLogger(RabbitMqConfiguration.class);

    @Value("${webprotege.rabbitmq.responsequeue}")
    public String RPC_RESPONSE_QUEUE;
    @Value("${webprotege.rabbitmq.requestqueue}")
    public String RPC_QUEUE1;

    public static final String RPC_EXCHANGE = "webprotege-exchange";

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired(required = false)
    private List<CommandHandler<? extends Request, ? extends Response>> commandHandlers = new ArrayList<>();

    @Autowired
    private PulsarCommandHandlerWrapperFactory wrapperFactory;
    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    @Lazy
    CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor;

    @Bean
    Queue msgQueue() {
        return new Queue(RPC_QUEUE1, true);
    }

    @Bean
    Queue replyQueue() {
        return new Queue(RPC_RESPONSE_QUEUE, true);
    }

    @Bean
    DirectExchange exchange() {
        return new DirectExchange(RPC_EXCHANGE, true, false);
    }


    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setReplyAddress(RPC_RESPONSE_QUEUE);
        rabbitTemplate.setReplyTimeout(60000);
        rabbitTemplate.setExchange(RPC_EXCHANGE);
        rabbitTemplate.setUseDirectReplyToContainer(false);
        return rabbitTemplate;
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
    public SimpleMessageListenerContainer messageListenerContainers(ConnectionFactory connectionFactory){

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setQueueNames(RPC_QUEUE1);
        container.setConnectionFactory(connectionFactory);
        container.setMessageListener(new RabbitMqHandlerWrapper<>(commandHandlers, objectMapper, authorizationStatusExecutor));
        logger.info("ALEX am construit message listener {}", RPC_QUEUE1);
        return container;
    }

    @Bean
    public List<Binding> bindings(DirectExchange directExchange, Queue msgQueue, Queue replyQueue){
        try (Connection connection = connectionFactory.createConnection();
             Channel channel = connection.createChannel(true)) {
            channel.exchangeDeclare(RPC_EXCHANGE, "direct", true);
            channel.queueDeclare(RPC_QUEUE1,true,false, false,null);
            channel.queueDeclare(RPC_RESPONSE_QUEUE,true,false, false,null);

            var response = new ArrayList<Binding>();

            for(CommandHandler handler: commandHandlers) {
                logger.info("Declaring binding queue {} to exchange {} with key {}",RPC_QUEUE1, RPC_EXCHANGE, handler.getChannelName());
                channel.queueBind(RPC_QUEUE1, RPC_EXCHANGE, handler.getChannelName());
                response.add(BindingBuilder.bind(msgQueue).to(directExchange).with(handler.getChannelName()));
            }
            channel.queueBind(RPC_RESPONSE_QUEUE, RPC_EXCHANGE,RPC_RESPONSE_QUEUE);

            response.add(BindingBuilder.bind(replyQueue).to(directExchange).with(replyQueue.getName()));
            channel.close();
            connection.close();
            return response;

        } catch (Exception e) {
            logger.error("Error ", e);
            throw new RuntimeException(e);
        }
    }


}
