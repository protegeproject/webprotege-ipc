package edu.stanford.protege.webprotege.ipc.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.ipc.GenericEventHandler;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-11
 */
@Configuration
public class PulsarGenericEventHandlersConfiguration {

    private static Logger logger = LoggerFactory.getLogger(PulsarGenericEventHandlersConfiguration.class);

    @Autowired(required = false)
    private List<GenericEventHandler> eventHandlers = new ArrayList<>();

    @Autowired
    ApplicationContext context;

    @PostConstruct
    private void postConstruct() {
        var wrapperFactory = context.getBean(PulsarGenericEventHandlerWrapperFactory.class);
        logger.info("Event handlers configuration:");
        eventHandlers.forEach(handler -> {
            logger.info("Auto-detected generic event handler: {}",
                        handler.getHandlerName());
            var wrapper = wrapperFactory.create(handler);
            wrapper.subscribe();
        });
    }

    @Bean
    PulsarGenericEventHandlerWrapperFactory pulsarGenericEventHandlerWrapperFactory(@Value("${spring.application.name}") String applicationName,
                                                                      ObjectMapper objectMapper,
                                                                      PulsarClient pulsarClient,
                                                                      @Value("${webprotege.pulsar.tenant}") String tenant) {
        return handler -> pulsarGenericEventHandlerWrapper(handler, applicationName, objectMapper, pulsarClient, tenant);
    }

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public PulsarGenericEventHandlerWrapper pulsarGenericEventHandlerWrapper(GenericEventHandler handler,
                                                                  String applicationName,
                                                                  ObjectMapper objectMapper, PulsarClient pulsarClient,
                                                                  @Value("${webprotege.pulsar.tenant}") String tenant) {
        return new PulsarGenericEventHandlerWrapper(applicationName, tenant, pulsarClient, handler, objectMapper);
    }
}
