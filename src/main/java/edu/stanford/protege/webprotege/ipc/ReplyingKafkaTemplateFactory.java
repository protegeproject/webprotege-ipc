package edu.stanford.protege.webprotege.ipc;

import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-03
 */
public interface ReplyingKafkaTemplateFactory {

    ReplyingKafkaTemplate<String, String, String> create(String replyingTopic);
}
