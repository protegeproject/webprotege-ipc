package edu.stanford.protege.webprotege.ipc;

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
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-10-08
 */
@Configuration
public class KafkaEventHandlersConfiguration implements KafkaListenerConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaCommandHandlersConfiguration.class);

    @Autowired(required = false)
    private List<EventHandler<?>> eventHandlers = new ArrayList<>();

    @Autowired
    private BeanFactory beanFactory;

    private final MessageHandlerMethodFactory messageHandlerMethodFactory = new DefaultMessageHandlerMethodFactory();

    @Value("${spring.kafka.consumer.group-id}")
    private String groupIdBase;

    @Value("${spring.application.name}")
    private String service;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        final var listenerMethod = lookUpMethod();
        eventHandlers.forEach(listener -> createAndRegisterListener(listener, listenerMethod, registrar));
    }

    private void createAndRegisterListener(final EventHandler<?> handler,
                                           final Method listenerMethod,
                                           final KafkaListenerEndpointRegistrar registrar) {
        logger.info("Registering {} endpoint on topic {}", handler.getClass(), handler.getChannelName());
        var endpoint = createListenerEndpoint(handler, listenerMethod);
        registrar.registerEndpoint(endpoint);
    }

    private MethodKafkaListenerEndpoint<String, String> createListenerEndpoint(final EventHandler<?> handler,
                                                                               final Method listenerMethod) {
        var listener = new KafkaListenerEventHandlerWrapper<>(objectMapper, handler);

        var endpoint = new MethodKafkaListenerEndpoint<String, String>();
        endpoint.setBeanFactory(beanFactory);
        endpoint.setBean(listener);
        endpoint.setMethod(listenerMethod);
        // Allow for multiple handlers for the same channel
        endpoint.setId(service + "-" + handler.getChannelName() + "-" + handler.getHandlerName());
        endpoint.setGroup(groupIdBase + "-" + handler.getHandlerName());
        endpoint.setTopics(handler.getChannelName());
        endpoint.setMessageHandlerMethodFactory(messageHandlerMethodFactory);
        return endpoint;
    }

    private Method lookUpMethod() {
        return Arrays.stream(KafkaListenerEventHandlerWrapper.class.getMethods())
                     .filter(m -> m.getName().equals(KafkaListenerEventHandlerWrapper.METHOD_NAME))
                     .findAny()
                     .orElseThrow(() -> new IllegalStateException("Could not find method " + KafkaListenerEventHandlerWrapper.METHOD_NAME));
    }

}
