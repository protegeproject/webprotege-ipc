package edu.stanford.protege.webprotege.ipc.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.ipc.cmd.Request;
import edu.stanford.protege.webprotege.ipc.cmd.Response;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.springframework.kafka.support.KafkaHeaders.*;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-06
 */
public class KafkaListenerHandlerWrapper<Q extends Request<R>, R extends Response> {

    public static final String METHOD_NAME = "handleMessage";

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerHandlerWrapper.class);

    private final KafkaTemplate<String, String> replyTemplate;

    private final ObjectMapper objectMapper;

    private final CommandHandler<Q, R> commandHandler;

    public KafkaListenerHandlerWrapper(KafkaTemplate<String, String> replyTemplate,
                                       ObjectMapper objectMapper,
                                       CommandHandler<Q, R> commandHandler) {
        this.replyTemplate = replyTemplate;
        this.objectMapper = objectMapper;
        this.commandHandler = commandHandler;
    }

    public void handleMessage(final ConsumerRecord<String, String> record) {
        var inboundHeaders = record.headers();
        var replyTopicHeader = inboundHeaders.lastHeader(REPLY_TOPIC);
        if (replyTopicHeader == null) {
            logger.error(REPLY_TOPIC + " header is missing.  Cannot reply to message.");
            return;
        }
        var correlationHeader = inboundHeaders.lastHeader(CORRELATION_ID);
        if(correlationHeader == null) {
            logger.error(CORRELATION_ID + " header is missing.  Cannot process message.");
            return;
        }

        var payload = record.value();

        var request = deserializeRequest(payload);

        if(request == null) {
            logger.debug("Unable to parse request.  Not handling message.");
            return;
        }
        // TODO: Handle execution exception
        var response = commandHandler.handleRequest(request);
        response.subscribe(r -> {
            var replyPayload = serializeResponse(r);
            if(replyPayload == null) {
                logger.debug("Unable to serialize response.  Not handling reply message.");
                return;
            }
            var replyHeaders = Arrays.<Header>asList(
                    new RecordHeader(TOPIC, replyTopicHeader.value()),
                    new RecordHeader(CORRELATION_ID, correlationHeader.value()));

            var reply = new ProducerRecord<>(new String(replyTopicHeader.value(), StandardCharsets.UTF_8),
                                             record.partition(),
                                             record.key(),
                                             replyPayload,
                                             replyHeaders);
            replyTemplate.send(reply);
        });

    }

    private Q deserializeRequest(String request) {
        try {
            return objectMapper.readValue(request, commandHandler.getRequestClass());
        } catch (JsonProcessingException e) {
            logger.debug("Error while deserializing request", e);
            return null;
        }
    }

    private String serializeResponse(R response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            logger.debug("Error while serializing response", e);
            return null;
        }
    }
}
