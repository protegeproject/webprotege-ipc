package edu.stanford.protege.webprotege.ipc.pulsar;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.pulsar.client.api.*;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.UncheckedIOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-02
 */
public class PulsarProducersManager {

    private static final Logger logger = LoggerFactory.getLogger(PulsarProducersManager.class);

    private final PulsarClient pulsarClient;

    private final String applicationName;

    private final Cache<String, Producer<byte[]>> cache = Caffeine.newBuilder()
                                                                  .expireAfterAccess(5, TimeUnit.MINUTES)
                                                                  .removalListener(this::handleProducerRemoved)
                                                                  .build();

    private void handleProducerRemoved(@Nullable String topicUrl,
                                       @Nullable Producer<byte[]> producer,
                                       @NonNull RemovalCause removalCause) {
        if (producer != null) {
            producer.closeAsync();
        }
    }

    public PulsarProducersManager(PulsarClient pulsarClient,
                                  @Value("${spring.application.name}") String applicationName) {
        this.pulsarClient = pulsarClient;
        this.applicationName = applicationName;
    }

    public Producer<byte[]> getProducer(String topicUrl,
                                        java.util.function.Consumer<ProducerBuilder<byte[]>> producerCustomizer) {
        return cache.get(topicUrl, u -> createProducer(u, producerCustomizer));
    }

    private Producer<byte[]> createProducer(String topicUrl, java.util.function.Consumer<ProducerBuilder<byte[]>> producerCustomizer) {
        try {
            var producerBuilder = pulsarClient.newProducer()
                                              .topic(topicUrl);
            producerCustomizer.accept(producerBuilder);
            return producerBuilder.create();
        } catch (PulsarClientException e) {
            logger.error("Error when creating Pulsar Producer", e);
            throw new UncheckedIOException(e);
        }
    }
}
