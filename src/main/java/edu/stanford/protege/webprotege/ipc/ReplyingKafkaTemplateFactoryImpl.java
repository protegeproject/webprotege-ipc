package edu.stanford.protege.webprotege.ipc;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-03
 */
public class ReplyingKafkaTemplateFactoryImpl implements ReplyingKafkaTemplateFactory {

    @Value("${spring.application.name}")
    private String applicationName;

    private final ConcurrentKafkaListenerContainerFactory<String, String> containerFactory;

    private final ProducerFactory<String, String> producerFactory;

    public ReplyingKafkaTemplateFactoryImpl(ConcurrentKafkaListenerContainerFactory<String, String> containerFactory,
                                            ProducerFactory<String, String> producerFactory) {
        this.containerFactory = containerFactory;
        this.producerFactory = producerFactory;
    }

    @Override
    public ReplyingKafkaTemplate<String, String, String> create(String replyingTopic) {
        var container = containerFactory.createContainer(replyingTopic);
        container.getContainerProperties().setGroupId(applicationName + "-" + replyingTopic + "-consumer");
        var template = new ReplyingKafkaTemplate<>(producerFactory, container);
        template.setSharedReplyTopic(true);
        template.setDefaultReplyTimeout(Duration.of(1, ChronoUnit.MINUTES));
        return template;
    }

}
