package edu.stanford.protege.webprotege.ipc.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-06
 */
@Configuration
public class KafkaListenerConfiguration implements KafkaListenerConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerConfiguration.class);

    @Autowired(required = false)
    private List<CommandHandler<?, ?>> commandHandlers = new ArrayList<>();

    @Autowired
    private BeanFactory beanFactory;

    private final MessageHandlerMethodFactory messageHandlerMethodFactory = new DefaultMessageHandlerMethodFactory();

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.application.name}")
    private String service;

    @Autowired
    private KafkaTemplate<String, String> replyTemplate;

    @Autowired
    private ObjectMapper objectMapper;



    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        final Method listenerMethod = lookUpMethod();
        commandHandlers.forEach(listener -> createAndRegisterListener(listener, listenerMethod, registrar));
    }

    private void createAndRegisterListener(final CommandHandler<?, ?> handler,
                                           final Method listenerMethod,
                                           final KafkaListenerEndpointRegistrar registrar) {
        logger.debug("Registering {} endpoint on topic {}", handler.getClass(), handler.getChannelName());
        var endpoint = createListenerEndpoint(handler, listenerMethod);
        registrar.registerEndpoint(endpoint);
    }

    private MethodKafkaListenerEndpoint<String, String> createListenerEndpoint(final CommandHandler<?,?> handler,
                                                                               final Method listenerMethod) {
        var listener = new KafkaListenerHandlerWrapper<>(replyTemplate, objectMapper, handler);

        var endpoint = new MethodKafkaListenerEndpoint<String, String>();
        endpoint.setBeanFactory(beanFactory);
        endpoint.setBean(listener);
        endpoint.setMethod(listenerMethod);
        endpoint.setId(service + "-" + handler.getChannelName());
        endpoint.setGroup(groupId);
        endpoint.setTopics(handler.getChannelName());
        endpoint.setMessageHandlerMethodFactory(messageHandlerMethodFactory);
        return endpoint;
    }

    private Method lookUpMethod() {
        return Arrays.stream(KafkaListenerHandlerWrapper.class.getMethods())
                     .filter(m -> m.getName().equals(KafkaListenerHandlerWrapper.METHOD_NAME))
                     .findAny()
                     .orElseThrow(() -> new IllegalStateException("Could not find method " + KafkaListenerHandlerWrapper.METHOD_NAME));
    }
}
