package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.common.UserId;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

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
        logger.info("Handling message: {}", record.headers());
        var inboundHeaders = record.headers();
        var replyHeaders = new ArrayList<Header>();

        var replyTopicHeader = inboundHeaders.lastHeader(REPLY_TOPIC);
        if (replyTopicHeader == null) {
            logger.error(REPLY_TOPIC + " header is missing.  Cannot reply to message.");
            return;
        }
        replyHeaders.add(new RecordHeader(TOPIC, replyTopicHeader.value()));

        var correlationHeader = inboundHeaders.lastHeader(CORRELATION_ID);
        if(correlationHeader == null) {
            logger.error(CORRELATION_ID + " header is missing.  Cannot process message.");
            return;
        }
        replyHeaders.add(new RecordHeader(CORRELATION_ID, correlationHeader.value()));

        var userIdHeader = inboundHeaders.lastHeader(Headers.USER_ID);
        if(userIdHeader == null) {
            logger.error(Headers.USER_ID + " header is missing.  Cannot process message.  Treating as unauthorized.");
            sendError(record, replyTopicHeader, replyHeaders, HttpStatus.UNAUTHORIZED);
            return;
        }
        replyHeaders.add(new RecordHeader(Headers.USER_ID, userIdHeader.value()));

        var payload = record.value();
        var request = deserializeRequest(payload);

        if(request == null) {
            logger.error("Unable to parse request");
            sendError(record, replyTopicHeader, replyHeaders, HttpStatus.BAD_REQUEST);
            return;
        }
        var userId = new UserId(new String(userIdHeader.value(), StandardCharsets.UTF_8));
        var executionContext = new ExecutionContext(userId);

        try {
            Mono<R> response = commandHandler.handleRequest(request, executionContext);
            response.subscribe(r -> {
                var replyPayload = serializeResponse(r);
                if(replyPayload == null) {
                    logger.error("Unable to serialize response");
                    sendError(record, replyTopicHeader, replyHeaders, HttpStatus.INTERNAL_SERVER_ERROR);
                    return;
                }
                var reply = new ProducerRecord<>(new String(replyTopicHeader.value(), StandardCharsets.UTF_8),
                                                 record.partition(),
                                                 record.key(),
                                                 replyPayload,
                                                 replyHeaders);
                replyTemplate.send(reply);
                logger.info("Sent reply to " + new String(replyTopicHeader.value()));
            }, throwable -> {
                if(throwable instanceof CommandExecutionException) {
                    sendError(record, replyTopicHeader, replyHeaders, ((CommandExecutionException) throwable).getStatus());
                }
                else {
                    sendError(record, replyTopicHeader, replyHeaders, HttpStatus.INTERNAL_SERVER_ERROR);
                }
            });
        } catch (Exception e) {
            logger.info("An unhandled exception occurred while executing an action {} {}",
                        e.getClass().getSimpleName(),
                        e.getMessage());
            sendError(record, replyTopicHeader, replyHeaders, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void sendError(ConsumerRecord<String, String> record,
                           Header replyTopicHeader,
                           ArrayList<Header> replyHeaders,
                           HttpStatus status) {
        replyHeaders.add(getErrorHeader(status));
        var reply = new ProducerRecord<String, String>(new String(replyTopicHeader.value(), StandardCharsets.UTF_8),
                                                       record.partition(),
                                                       record.key(),
                                                       status.getReasonPhrase(), replyHeaders);
        replyTemplate.send(reply);
    }

    private RecordHeader getErrorHeader(HttpStatus status) {
        var errorValue = serializeCommandExecutionException(new CommandExecutionException(status));
        return new RecordHeader(Headers.ERROR, errorValue.getBytes(StandardCharsets.UTF_8));
    }

    private UserId getUserIdFromAccessToken(byte[] tokenBytes) {
        var token = new String(tokenBytes, StandardCharsets.UTF_8);
        String[] chunks = token.split("\\.");
        var decoder = Base64.getDecoder();
        String header = new String(decoder.decode(chunks[0]));
        String payload = new String(decoder.decode(chunks[1]));
        return new UserId("JohnSmith");
    }

    private Q deserializeRequest(String request) {
        try {
            return objectMapper.readValue(request, commandHandler.getRequestClass());
        } catch (JsonProcessingException e) {
            logger.error("Error while deserializing request", e);
            return null;
        }
    }

    private String serializeResponse(R response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            logger.error("Error while serializing response", e);
            return null;
        }
    }

    private String serializeCommandExecutionException(CommandExecutionException exception) {
        try {
            return objectMapper.writeValueAsString(exception);
        } catch (JsonProcessingException e) {
            logger.error("Error while serializing CommandExecutionException", e);
            return """
                    {
                        "status" : 500
                    }
                    """.strip();
        }

    }
}
