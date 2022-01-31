package edu.stanford.protege.webprotege.ipc.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.CommandExecutor;
import edu.stanford.protege.webprotege.ipc.ExecutionContext;
import edu.stanford.protege.webprotege.ipc.Headers;
import edu.stanford.protege.webprotege.ipc.MessageChannelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.messaging.support.MessageBuilder;

import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;

import static org.springframework.kafka.support.KafkaHeaders.REPLY_TOPIC;
import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-01-31
 */
public class KafkaCommandExecutor<Q extends Request<R>, R extends Response> implements CommandExecutor<Q, R> {

    private ReplyingKafkaTemplate<String, String, String> template = null;

    @Autowired
    private ReplyingKafkaTemplateFactory templateFactory;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MessageChannelMapper channelMapper;

    private final Class<R> responseClass;

    public KafkaCommandExecutor(Class<R> responseClass) {
        this.responseClass = responseClass;
    }

    public CompletableFuture<R> execute(Q request, ExecutionContext executionContext) {
        try {
            var replyTopic = channelMapper.getReplyChannelName(request);
            ensureFactory(replyTopic);
            var json = objectMapper.writeValueAsString(request);
            var topic = channelMapper.getChannelName(request);
            var msg = MessageBuilder.withPayload(json)
                                    .setHeader(REPLY_TOPIC, replyTopic)
                                    .setHeader(TOPIC, topic)
                                    .setHeader(Headers.USER_ID, executionContext.userId().value())
                                    .build();
            var replyFuture = template.sendAndReceive(msg);
            // TODO:  Wrap Future
            return replyFuture.completable().thenApply(f -> {
                try {
                    var replyJson = (String) f.getPayload();
                    return objectMapper.readValue(replyJson, responseClass);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    private synchronized void ensureFactory(String replyTopic) {
        if(template == null) {
            template = templateFactory.create(replyTopic);
            template.start();
        }
    }
}
