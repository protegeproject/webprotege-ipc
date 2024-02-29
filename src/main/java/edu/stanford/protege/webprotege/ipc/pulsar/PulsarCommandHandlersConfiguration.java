package edu.stanford.protege.webprotege.ipc.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-01
 */
@Configuration
public class PulsarCommandHandlersConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(PulsarCommandHandlersConfiguration.class);

    @Autowired(required = false)
    private List<CommandHandler<? extends Request, ? extends Response>> commandHandlers = new ArrayList<>();

    @Autowired
    private PulsarCommandHandlerWrapperFactory wrapperFactory;

    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor;

    @Autowired
    private ConnectionFactory connectionFactory;

    public static final String RPC_QUEUE1 = "webprotege-rpc-queue";
    public static final String RPC_RESPONSE_QUEUE = "webprotege-backend-response-queue";

    public static final String RPC_EXCHANGE = "webprotege-exchange";

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
    public List<Binding> bindings(DirectExchange directExchange, Queue msgQueue){
        try (Connection connection = connectionFactory.createConnection();
             Channel channel = connection.createChannel(true)) {
            channel.exchangeDeclare(RPC_EXCHANGE, "direct", true);
            channel.queueDeclare(RPC_QUEUE1,true,false, false,null);

            var response = new ArrayList<Binding>();

            for(CommandHandler handler: commandHandlers) {
                logger.info("Declaring binding {} {} " + handler.getChannelName());
                channel.queueBind(RPC_QUEUE1, RPC_EXCHANGE, handler.getChannelName());
                response.add(BindingBuilder.bind(msgQueue).to(directExchange).with(handler.getChannelName()));
            }
            return response;

        } catch (Exception e) {
            logger.error("Error ", e);
            throw new RuntimeException(e);
        }


    }

    @Bean
    public SimpleMessageListenerContainer messageListenerContainers(ConnectionFactory connectionFactory){

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setQueueNames(RPC_QUEUE1);
        container.setConnectionFactory(connectionFactory);
        container.setMessageListener(new RabbitMqHandlerWrapper<>(commandHandlers, objectMapper, authorizationStatusExecutor, rabbitTemplate));

        return container;
    }

    @PostConstruct
    private void postConstruct() {
        logger.info("Command handlers configuration:");
        commandHandlers.forEach(handler -> {
                logger.info("Auto-detected command handler {} for channel {}",
                            handler.getClass().getName(),
                            handler.getChannelName());
                var wrapper = wrapperFactory.create(handler);
                wrapper.subscribe();
        });
    }
}
