package edu.stanford.protege.webprotege.ipc.pulsar;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.ProjectRequest;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.CommandExecutor;
import edu.stanford.protege.webprotege.ipc.ExecutionContext;
import edu.stanford.protege.webprotege.ipc.Headers;
import edu.stanford.protege.webprotege.ipc.MessageChannelMapper;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-01-31
 *
 * A {@link PulsarCommandExecutor} is used to execute a specific command that has a specific type of request and
 * a specific type of response.  That is, a given command executor instance only handles requests for single channel.
 */
public class PulsarCommandExecutor<Q extends Request<R>, R extends Response> implements CommandExecutor<Q, R> {

    private static final String NAMESPACE = "commands";

    private static final Logger logger = LoggerFactory.getLogger(PulsarCommandExecutor.class);

    private final Class<R> responseClass;

    @Autowired
    private String applicationName;

    @Autowired
    private PulsarClient pulsarClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MessageChannelMapper channelMapper;


    private Producer<byte []> producer;

    private Consumer<byte []> consumer;

    private String requestChannel = null;

    private String replyChannel = null;

    private final Map<String, CompletableFuture<R>> replyHandlers = new ConcurrentHashMap<>();


    public PulsarCommandExecutor(Class<R> responseClass) {
        this.responseClass = responseClass;
    }

    @Override
    public CompletableFuture<R> execute(Q request, ExecutionContext executionContext) {
        try {
            var replyChannel = channelMapper.getReplyChannelName(request);
            var json = objectMapper.writeValueAsBytes(request);
            try {
                var producer = getProducer(request);
                var key = getKeyForRequest(request);
                var correlationId = UUID.randomUUID().toString();
                // TODO: Place correlation Id in a map
                var replyFuture = new CompletableFuture<R>();
                replyHandlers.put(correlationId, replyFuture);
                producer.newMessage()
                        // If project request
                        .key(key)
                        .value(json)
                        .property(Headers.CORRELATION_ID, correlationId)
                        .property(Headers.REPLY_CHANNEL, replyChannel)
                        .property(Headers.USER_ID, executionContext.userId().value())
                        // TODO: ProjectId
                        .property(Headers.PROJECT_ID, "")
                        .send();
                return replyFuture;
            } catch (PulsarClientException e) {
                e.printStackTrace();
                return new CompletableFuture<>();
            }
        } catch (JsonProcessingException e) {
            logger.error("JSON Processing Exception");
            throw new UncheckedIOException(e);
        }
    }

    private String getKeyForRequest(Q request) {
        return request instanceof ProjectRequest ? ((ProjectRequest<?>) request).projectId().value() : null;
    }

    private synchronized Producer<byte []> getProducer(Q request) {
        try {
            if(producer != null) {
                return producer;
            }
            ensureConsumerIsListeningForRepliesToRequest(request);
            if(requestChannel == null) {
                requestChannel = channelMapper.getChannelName(request);
            }
            if(!this.requestChannel.equals(request.getChannel())) {
                throw new RuntimeException("Request channel is not the request channel that is in use by this CommandExecutor");
            }
            var topicName = "persistent://webprotege/" + NAMESPACE + "/" + requestChannel;
            return producer = pulsarClient.newProducer()
                    .producerName(applicationName)
                    .topic(topicName)
                    // TODO: SendTimeout
                    .create();
        } catch (PulsarClientException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void ensureConsumerIsListeningForRepliesToRequest(Q request) {
        try {
            if(consumer != null) {
                return;
            }
            if(this.replyChannel == null) {
                this.replyChannel = channelMapper.getReplyChannelName(request);
            }
            if(!this.replyChannel.equals(channelMapper.getReplyChannelName(request))) {
                throw new RuntimeException("Reply channel is not the channel that is in use by this command executor");
            }
            var replyTopic = "persistent://webprotege/" + NAMESPACE + "/" + replyChannel;
            consumer = pulsarClient.newConsumer()
                                   .consumerName(applicationName)
                                   .topic(replyTopic)
                                   .messageListener(this::handleReplyMessageReceived)
                                   .subscribe();
        } catch (PulsarClientException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void handleReplyMessageReceived(Consumer<byte[]> consumer, Message<byte[]> msg) {
        try {
            var correlationId = msg.getProperty(Headers.CORRELATION_ID);
            if(correlationId == null) {
                logger.info("CorrelationId in reply message is missing.  Cannot handle reply.  Ignoring reply.");
                return;
            }
            var replyHandler = replyHandlers.remove(correlationId);
            var response = objectMapper.readValue(msg.getData(), responseClass);
            consumer.acknowledge(msg);
            replyHandler.complete(response);
        } catch (PulsarClientException e) {
            logger.error("Encountered Pulsar Client Exception", e);
            throw new UncheckedIOException(e);
        } catch (IOException e ) {
            logger.error("Cannot deserialize reply message on topic {}", consumer.getTopic(), e);
            consumer.negativeAcknowledge(msg);
        }
    }

    @PreDestroy
    protected void preDestroy() {
        if (consumer != null) {
            logger.info("Closing {} consumer listening to {}", applicationName, consumer.getTopic());
            consumer.closeAsync();
        }
        if (producer != null) {
            logger.info("Closing {} producer that publishes messages to {}", applicationName, producer.getTopic());
            producer.closeAsync();
        }
    }


}
