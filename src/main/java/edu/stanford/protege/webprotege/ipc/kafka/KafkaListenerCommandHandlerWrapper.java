package edu.stanford.protege.webprotege.ipc.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.authorization.AuthorizationStatus;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.authorization.Subject;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.common.UserId;
import edu.stanford.protege.webprotege.ipc.*;
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

import static org.springframework.kafka.support.KafkaHeaders.*;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-06
 */
public class KafkaListenerCommandHandlerWrapper<Q extends Request<R>, R extends Response> {

    public static final String METHOD_NAME = "handleMessage";

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerCommandHandlerWrapper.class);

    private final KafkaTemplate<String, String> replyTemplate;

    private final ObjectMapper objectMapper;

    private final CommandHandler<Q, R> commandHandler;

    private final CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> executor;

    public KafkaListenerCommandHandlerWrapper(KafkaTemplate<String, String> replyTemplate,
                                              ObjectMapper objectMapper,
                                              CommandHandler<Q, R> commandHandler,
                                              CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> executor) {
        this.replyTemplate = replyTemplate;
        this.objectMapper = objectMapper;
        this.commandHandler = commandHandler;
        this.executor = executor;
    }

    public void handleMessage(final ConsumerRecord<String, String> record) {
        var inboundHeaders = record.headers();
        var replyHeaders = new ArrayList<Header>();

        var replyTopicHeader = inboundHeaders.lastHeader(REPLY_TOPIC);
        if (replyTopicHeader == null) {
            logger.error(REPLY_TOPIC + " header is missing.  Cannot reply to message.");
            return;
        }
        replyHeaders.add(new RecordHeader(TOPIC, replyTopicHeader.value()));

        var correlationHeader = inboundHeaders.lastHeader(CORRELATION_ID);
        if (correlationHeader == null) {
            logger.error(CORRELATION_ID + " header is missing.  Cannot process message.");
            return;
        }
        replyHeaders.add(new RecordHeader(CORRELATION_ID, correlationHeader.value()));

        var userIdHeader = inboundHeaders.lastHeader(Headers.USER_ID);
        if (userIdHeader == null) {
            logger.error(Headers.USER_ID + " header is missing.  Cannot process message.  Treating as unauthorized.  Message reply topic: {}",
                         new String(replyTopicHeader.value(), StandardCharsets.UTF_8));
            sendError(record, replyTopicHeader, replyHeaders, HttpStatus.UNAUTHORIZED);
            return;
        }

        //        var accessToken = inboundHeaders.lastHeader(Headers.ACCESS_TOKEN);
        //        if (accessToken == null) {
        //            logger.error(Headers.ACCESS_TOKEN + " header is missing.  Cannot process message.  Treating as unauthorized.  Message reply topic: {}", new String(replyTopicHeader.value(), StandardCharsets.UTF_8));
        //            sendError(record, replyTopicHeader, replyHeaders, HttpStatus.UNAUTHORIZED);
        //            return;
        //        }

        replyHeaders.add(new RecordHeader(Headers.USER_ID, userIdHeader.value()));
        //        replyHeaders.add(new RecordHeader(Headers.ACCESS_TOKEN, accessToken.value()));


        var payload = record.value();
        var request = deserializeRequest(payload);

        if (request == null) {
            logger.error("Unable to parse request");
            sendError(record, replyTopicHeader, replyHeaders, HttpStatus.BAD_REQUEST);
            return;
        }
        var userId = new UserId(new String(userIdHeader.value(), StandardCharsets.UTF_8));
        //        var jwt = new String(accessToken.value(), StandardCharsets.UTF_8);

        //        verifyJwt(jwt);

        var executionContext = new ExecutionContext(userId, "");

        // Check authorized
        if (commandHandler instanceof AuthorizedCommandHandler<Q, R> authenticatingCommandHandler) {
            var resource = authenticatingCommandHandler.getTargetResource(request);
            var subject = Subject.forUser(userId);
            var requiredActionId = authenticatingCommandHandler.getRequiredCapabilities();
            // Call to the authorization service to check
            var statusResponse = executor.execute(new GetAuthorizationStatusRequest(resource,
                                                                                    subject,
                                                                                    requiredActionId.stream()
                                                                                                    .findFirst()
                                                                                                    .orElse(null)),
                                                  executionContext);
            statusResponse.whenComplete((r, e) -> {
                if (e != null) {
                    logger.warn("An error occurred when requesting the authorization status for {} on {}. Error: {}",
                                userId,
                                resource,
                                e.getMessage());
                    sendError(record, replyTopicHeader, replyHeaders, HttpStatus.INTERNAL_SERVER_ERROR);
                }
                else {
                    if (r.authorizationStatus() == AuthorizationStatus.AUTHORIZED) {
                        handleAuthorizedRequest(record, replyHeaders, replyTopicHeader, request, executionContext);
                    }
                    else {
                        logger.info("Permission denied when attempting to execute a request.  User: {}, Request: {}",
                                    userId,
                                    request);
                        sendError(record, replyTopicHeader, replyHeaders, HttpStatus.FORBIDDEN);
                    }
                }

            });
        }
        else {
            handleAuthorizedRequest(record, replyHeaders, replyTopicHeader, request, executionContext);
        }
    }

    private void handleAuthorizedRequest(ConsumerRecord<String, String> record,
                                         ArrayList<Header> replyHeaders,
                                         Header replyTopicHeader,
                                         Q request,
                                         ExecutionContext executionContext) {
        Mono<R> response = commandHandler.handleRequest(request, executionContext);
        response.subscribe(r -> {
            var replyPayload = serializeResponse(r);
            if (replyPayload == null) {
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
            if (throwable instanceof CommandExecutionException ex) {
                logger.info(
                        "The command handler threw a CommandExecutionException exception while handling a request.  Sending an error as the reply to {}.  Code: {}, Message: {},  Request: {}",
                        new String(replyTopicHeader.value()),
                        ex.getStatusCode(),
                        throwable.getMessage(),
                        request);
                sendError(record, replyTopicHeader, replyHeaders, ex.getStatus());
            }
            else {
                logger.info(
                        "The command handler threw an exception while handling a request.  Sending an error as the reply to {}.  Exception class: {}, Message: {},  Request: {}",
                        new String(replyTopicHeader.value()),
                        throwable.getClass().getName(),
                        throwable.getMessage(),
                        request);
                sendError(record, replyTopicHeader, replyHeaders, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        });
    }

    private void sendError(ConsumerRecord<String, String> record,
                           Header replyTopicHeader,
                           ArrayList<Header> replyHeaders,
                           HttpStatus status) {
        replyHeaders.add(getErrorHeader(status));
        var reply = new ProducerRecord<>(new String(replyTopicHeader.value(), StandardCharsets.UTF_8),
                                         record.partition(),
                                         record.key(),
                                         status.getReasonPhrase(),
                                         replyHeaders);
        replyTemplate.send(reply);
    }

    private RecordHeader getErrorHeader(HttpStatus status) {
        var errorValue = serializeCommandExecutionException(new CommandExecutionException(status));
        return new RecordHeader(Headers.ERROR, errorValue.getBytes(StandardCharsets.UTF_8));
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
                        "statusCode" : 500
                    }
                    """.strip();
        }

    }

    private void verifyJwt(String token) {
        //        try {
        //            DecodedJWT jwt = JWT.decode(token);
        //
        //            // check JWT is valid
        //            Jwk jwk = jwtService.getJwk();
        //            Algorithm algorithm = Algorithm.RSA256((RSAPublicKey) jwk.getPublicKey(), null);
        //
        //            algorithm.verify(jwt);
        //
        //            // check JWT is still active
        //            Date expiryDate = jwt.getExpiresAt();
        //            if (expiryDate.before(new Date())) {
        //                throw new Exception("token is expired");
        //            }
        //
        //
        //        } catch (Exception e) {
        //            logger.error("exception : {} ", e.getMessage());
        //            throw new RuntimeException();
        //        }
        return;
    }
}
