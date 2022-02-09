package edu.stanford.protege.webprotege.ipc.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.ipc.EventDispatcher;
import edu.stanford.protege.webprotege.ipc.EventRecordHandler;
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
 * 2022-01-08
 */
//@Configuration
public class KafkaEventRecordHandlersConfiguration implements KafkaListenerConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEventRecordHandlersConfiguration.class);

    @Autowired(required = false)
    private List<EventRecordHandler> eventHandlers = new ArrayList<>();

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

    private void createAndRegisterListener(final EventRecordHandler handler,
                                           final Method listenerMethod,
                                           final KafkaListenerEndpointRegistrar registrar) {
        logger.info("Registering {} endpoint on topic {}", handler.getClass(), EventDispatcher.WEBPROTEGE_EVENTS_CHANNEL_NAME);
        var endpoint = createListenerEndpoint(handler, listenerMethod);
        registrar.registerEndpoint(endpoint);
    }

    private MethodKafkaListenerEndpoint<String, String> createListenerEndpoint(final EventRecordHandler handler,
                                                                               final Method listenerMethod) {
        var listener = new KafkaListenerEventRecordHandlerWrapper(handler);

        var endpoint = new MethodKafkaListenerEndpoint<String, String>();
        endpoint.setBeanFactory(beanFactory);
        endpoint.setBean(listener);
        endpoint.setMethod(listenerMethod);
        // Allow for multiple handlers for the same channel
        endpoint.setId(service + "-" + EventDispatcher.WEBPROTEGE_EVENTS_CHANNEL_NAME + "-" + handler.getHandlerName());
        endpoint.setGroup(groupIdBase + "-" + handler.getHandlerName());
        endpoint.setTopics(EventDispatcher.WEBPROTEGE_EVENTS_CHANNEL_NAME);
        endpoint.setMessageHandlerMethodFactory(messageHandlerMethodFactory);
        return endpoint;
    }

    private Method lookUpMethod() {
        return Arrays.stream(KafkaListenerEventRecordHandlerWrapper.class.getMethods())
                     .filter(m -> m.getName().equals(KafkaListenerEventRecordHandlerWrapper.METHOD_NAME))
                     .findAny()
                     .orElseThrow(() -> new IllegalStateException("Could not find method " + KafkaListenerEventRecordHandlerWrapper.METHOD_NAME));
    }
}
