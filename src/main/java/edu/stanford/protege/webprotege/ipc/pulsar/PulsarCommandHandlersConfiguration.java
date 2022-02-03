package edu.stanford.protege.webprotege.ipc.pulsar;

import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.ipc.*;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-01
 */
@Configuration
public class PulsarCommandHandlersConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(PulsarCommandHandlersConfiguration.class);

    @Autowired(required = false)
    private List<CommandHandler<?, ?>> commandHandlers = new ArrayList<>();

    @Autowired
    private PulsarCommandHandlerWrapperFactory wrapperFactory;


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
