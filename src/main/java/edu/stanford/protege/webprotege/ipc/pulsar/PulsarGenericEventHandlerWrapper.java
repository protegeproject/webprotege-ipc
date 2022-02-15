package edu.stanford.protege.webprotege.ipc.pulsar;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.ProjectId;
import edu.stanford.protege.webprotege.ipc.EventRecord;
import edu.stanford.protege.webprotege.ipc.GenericEventHandler;
import edu.stanford.protege.webprotege.ipc.Headers;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-10
 */
public class PulsarGenericEventHandlerWrapper {

    private static final Logger logger = LoggerFactory.getLogger(PulsarGenericEventHandlerWrapper.class);

    private final String applicationName;

    private final String tenant;

    private final PulsarClient pulsarClient;

    private final GenericEventHandler handler;

    private final ObjectMapper objectMapper;


    public PulsarGenericEventHandlerWrapper(@Value("${spring.application.name}") String applicationName,
                                            @Value("${webprotege.pulsar.tenant}") String tenant,
                                            PulsarClient pulsarClient,
                                            GenericEventHandler handler,
                                            ObjectMapper objectMapper) {
        this.applicationName = applicationName;
        this.tenant = tenant;
        this.pulsarClient = pulsarClient;
        this.handler = handler;
        this.objectMapper = objectMapper;
    }

    public void subscribe() {
        try {
            var subscriptionName = applicationName + "--" + handler.getHandlerName();
            var consumerName = applicationName + "--" + handler.getHandlerName() + "--Consumer";
            logger.info("Subscribing consumer {} to {}", consumerName, GenericEventHandler.ALL_EVENTS_CHANNEL);
            var topicUrl = tenant + "/" + PulsarNamespaces.EVENTS + "/" + GenericEventHandler.ALL_EVENTS_CHANNEL;
            pulsarClient.newConsumer()
                        .subscriptionType(SubscriptionType.Shared)
                        .subscriptionName(subscriptionName)
                        .consumerName(consumerName)
                        .topic(topicUrl)
                        .messageListener(this::handleMessage)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe();
            handler.handlerSubscribed();
        } catch (PulsarClientException e) {
            logger.error("Could not subscribe to event topics", e);
        }
    }

    private void handleMessage(Consumer<byte[]> consumer, Message<byte[]> message) {
        try {
            var eventType = message.getProperty(Headers.EVENT_TYPE);
            if (eventType == null) {
                logger.warn("Cound not handle event message because {} header is missing", Headers.EVENT_TYPE);
                consumer.acknowledge(message);
                return;
            }
            var eventRecord = objectMapper.readValue(message.getValue(), EventRecord.class);
            consumer.acknowledge(message);
            try {
                handler.handleEventRecord(eventRecord);
            } catch (Exception e) {
                logger.error("Handled exception thrown by EventRecord handler: {}", handler.getHandlerName(), e);
            }
        } catch (IOException e) {
            logger.error("An error occurred reading an event record", e);
        }
    }
}
