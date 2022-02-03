package edu.stanford.protege.webprotege.ipc.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Event;
import edu.stanford.protege.webprotege.ipc.EventHandler;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-03
 */
public class PulsarEventHandlerWrapper<E extends Event> implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(PulsarEventHandlerWrapper.class);

    private final String applicationName;

    private final EventHandler<E> eventHandler;

    private final ObjectMapper objectMapper;

    private final PulsarClient pulsarClient;

    private Consumer<byte[]> consumer;

    public PulsarEventHandlerWrapper(String applicationName,
                                     EventHandler<E> eventHandler,
                                     ObjectMapper objectMapper,
                                     PulsarClient pulsarClient) {
        this.applicationName = applicationName;
        this.eventHandler = eventHandler;
        this.objectMapper = objectMapper;
        this.pulsarClient = pulsarClient;
    }

    public void subscribe() {
        if(consumer != null) {
            logger.info("Already subscribed.  Not subscribing again.");
            return;
        }
        try {
            var eventTopicUrl = TopicUrl.getEventTopicUrl(eventHandler.getChannelName());
            var subscriptionName = getSubscriptionName();
            consumer = pulsarClient.newConsumer()
                                   .subscriptionName(subscriptionName)
                                   .subscriptionType(SubscriptionType.Shared)
                                   .topic(eventTopicUrl)
                                   // TODO: Dead letter topic
                                   .messageListener(this::handleMessage)
                                   .subscribe();
        } catch (PulsarClientException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String getSubscriptionName() {
        return applicationName + "-" + eventHandler.getChannelName() + "-" + eventHandler.getHandlerName();
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }

    private void handleMessage(Consumer<byte[]> consumer, Message<byte[]> msg) {
        try {
            var event = objectMapper.readValue(msg.getData(), eventHandler.getEventClass());
            consumer.acknowledge(msg);
            handleEvent(event);
        } catch (IOException e) {
            logger.error("Could not parse event on channel {} with class {}",
                         eventHandler.getChannelName(),
                         eventHandler.getEventClass().getName(),
                         e);
            consumer.negativeAcknowledge(msg);
        }

    }

    private void handleEvent(E event) {
        try {
            eventHandler.handleEvent(event);
        } catch (Exception e) {
            logger.warn("Caught unhandled exception thrown from event handler. Event hangler name: {}.  Message: {}", eventHandler.getHandlerName(), e.getMessage(), e);
        }
    }
}
