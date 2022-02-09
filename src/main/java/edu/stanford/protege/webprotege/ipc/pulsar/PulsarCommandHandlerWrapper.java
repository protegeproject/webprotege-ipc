package edu.stanford.protege.webprotege.ipc.pulsar;

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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.UncheckedIOException;

import static edu.stanford.protege.webprotege.ipc.Headers.ERROR;
import static edu.stanford.protege.webprotege.ipc.Headers.USER_ID;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-02
 */
public class PulsarCommandHandlerWrapper<Q extends Request<R>, R extends Response> {

    private static final Logger logger = LoggerFactory.getLogger(PulsarCommandHandlerWrapper.class);

    private final String applicationName;

    private final String tenant;

    private final PulsarClient pulsarClient;

    private final CommandHandler<Q, R> handler;

    private final ObjectMapper objectMapper;

    private final PulsarProducersManager producersManager;

    private final CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor;

    private Consumer<byte[]> consumer;

    public PulsarCommandHandlerWrapper(String applicationName,
                                       @Value("webprotege.pulsar.tenant") String tenant,
                                       PulsarClient pulsarClient,
                                       CommandHandler<Q, R> handler,
                                       ObjectMapper objectMapper,
                                       PulsarProducersManager producersManager,
                                       CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor) {
        this.applicationName = applicationName;
        this.tenant = tenant;
        this.pulsarClient = pulsarClient;
        this.handler = handler;
        this.objectMapper = objectMapper;
        this.producersManager = producersManager;
        this.authorizationStatusExecutor = authorizationStatusExecutor;
    }

    @PreDestroy
    public void unsubscribe() {
        try {
            consumer.unsubscribe();
        } catch (PulsarClientException e) {
            logger.warn("An exception was thrown when unsubscribing", e);
        }
    }

    public void subscribe() {
        try {
            consumer = pulsarClient.newConsumer()
                                        .topic(getRequestsTopicUrl(handler))
                                        .subscriptionName(getSubscriptionName(handler))
                                        .consumerName(getConsumerName(handler))
                                        .messageListener(this::handleCommandMessage)
                                        .subscribe();
        } catch (PulsarClientException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void handleCommandMessage(Consumer<byte[]> consumer, Message<byte[]> message) {

        // Check vital headers are present (replyChannel, correlationId, userId, JWT?).  If vital headers are not
        // present then the call fails.
        // DO WE REPLY OR LOG AND TIMEOUT?

        // Next, we parse the request.
        // If the request cannot be parsed then we need to send a BAD_REQUEST error

        // Next we check the request is authorized.

        // If the authorization check fails then we send back an error

        // If the request is not authorized then we send back an error

        // Next we handle the actual request using the handler.
        // If a runtime exception is thrown while handling the request then we need to send back an ERROR.  Which one?  What does Spring do?
        // If the return value is an exception then...
        // If the return value is not an exception then we serialize it.


        var replyChannel = message.getProperty(Headers.REPLY_CHANNEL);
        if (replyChannel == null) {
            logger.error(Headers.REPLY_CHANNEL + " header is missing.  Cannot reply to message.");
            // TODO: Send ERROR
            consumer.negativeAcknowledge(message);
            return;
        }

        var correlationId = message.getProperty(Headers.CORRELATION_ID);
        if (correlationId == null) {
            logger.error(Headers.CORRELATION_ID + " header is missing.  Cannot process message.");
            // TODO: Send ERROR
            consumer.negativeAcknowledge(message);
            return;
        }

        var userId = message.getProperty(USER_ID);
        if (userId == null) {
            logger.error(USER_ID + " header is missing.  Cannot process message.  Treating as unauthorized.  Message reply topic: {}",
                         replyChannel);
            //                sendError(record, replyChannel, replyHeaders, HttpStatus.UNAUTHORIZED);
            consumer.negativeAcknowledge(message);
            return;
        }


        //        var accessToken = inboundHeaders.lastHeader(Headers.ACCESS_TOKEN);
        //        if (accessToken == null) {
        //            logger.error(Headers.ACCESS_TOKEN + " header is missing.  Cannot process message.  Treating as unauthorized.  Message reply topic: {}", new String(replyTopicHeader.value(), StandardCharsets.UTF_8));
        //            sendError(record, replyTopicHeader, replyHeaders, HttpStatus.UNAUTHORIZED);
        //            return;
        //        }
        //        replyHeaders.add(new RecordHeader(Headers.ACCESS_TOKEN, accessToken.value()));


        parseAndHandleRequest(consumer, message, replyChannel, correlationId, userId);
    }

    private void parseAndHandleRequest(Consumer<byte[]> consumer,
                                       Message<byte[]> message,
                                       String replyChannel,
                                       String correlationId,
                                       String userId) {
        try {
            var payload = message.getData();
            var request = objectMapper.readValue(payload, handler.getRequestClass());
            // The request has successfully been read.  All required headers are present and the request body
            // is well-formed so acknowledge the request (i.e. it shouldn't be dead-lettered)
            consumer.acknowledgeAsync(message);

            if (handler instanceof AuthorizedCommandHandler<Q, R> authorizedCommandHandler) {
                authorizeAndReplyToRequest(replyChannel, correlationId, userId, request, authorizedCommandHandler);
            }
            else {
                handleAndReplyToRequest(replyChannel, correlationId, userId, request);
            }

        } catch (IOException e) {
            logger.error("Could not parse request", e);
            consumer.negativeAcknowledge(message);
            replyWithErrorResponse(replyChannel, correlationId, userId, HttpStatus.BAD_REQUEST);
        }
    }

    private void authorizeAndReplyToRequest(String replyChannel,
                                            String correlationId,
                                            String userId,
                                            Q request,
                                            AuthorizedCommandHandler<Q, R> authenticatingCommandHandler) {
        var resource = authenticatingCommandHandler.getTargetResource(request);
        var subject = Subject.forUser(userId);
        var requiredActionId = authenticatingCommandHandler.getRequiredCapabilities();
        // Call to the authorization service to check
        var authRequest = new GetAuthorizationStatusRequest(resource,
                                                            subject,
                                                            requiredActionId.stream().findFirst().orElse(null));
        var executionContext = new ExecutionContext(new UserId(userId), "");
        var authResponseFuture = authorizationStatusExecutor.execute(authRequest, executionContext);
        authResponseFuture.whenComplete((authResponse, authError) -> {
            if (authError != null) {
                // The call to the authorization service failed
                logger.warn("An error occurred when requesting the authorization status for {} on {}. Error: {}",
                            userId,
                            resource,
                            authError.getMessage());
                // Upstream Error
                replyWithErrorResponse(replyChannel, correlationId, userId, HttpStatus.INTERNAL_SERVER_ERROR);
            }
            else {
                // The call to the authorization service succeeded
                if (authResponse.authorizationStatus() == AuthorizationStatus.AUTHORIZED) {
                    handleAndReplyToRequest(replyChannel, correlationId, userId, request);
                }
                else {
                    logger.info("Permission denied when attempting to execute a request.  User: {}, Request: {}",
                                userId,
                                request);
                    replyWithErrorResponse(replyChannel, correlationId, userId, HttpStatus.FORBIDDEN);
                }
            }

        });
    }

    private void handleAndReplyToRequest(String replyChannel, String correlationId, String userId, Q request) {
        var executionContext = new ExecutionContext(new UserId(userId), "");
        var response = handler.handleRequest(request, executionContext);
        response.subscribe(r -> {
            replyWithSuccessResponse(replyChannel, correlationId, userId, r);
            logger.info("Sent reply to {}", replyChannel);
        }, throwable -> {
            if (throwable instanceof CommandExecutionException ex) {
                logger.info(
                        "The command handler threw a CommandExecutionException exception while handling a request.  Sending an error as the reply to {}.  Code: {}, Message: {},  Request: {}",
                        replyChannel,
                        ex.getStatusCode(),
                        throwable.getMessage(),
                        request);
                replyWithErrorResponse(replyChannel, correlationId, userId, ex.getStatus());
            }
            else {
                logger.info(
                        "The command handler threw an exception while handling a request.  Sending an error as the reply to {}.  Exception class: {}, Message: {},  Request: {}",
                        replyChannel,
                        throwable.getClass().getName(),
                        throwable.getMessage(),
                        request);
                replyWithErrorResponse(replyChannel, correlationId, userId, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        });
    }

    /**
     * Replies with an error response.  This means that an error header (#ERROR) is set whose value is a serialized
     * HTTP Status object.
     * @param status The status that describes the error
     */
    private void replyWithErrorResponse(String replyChannel, String correlationId, String userId, HttpStatus status) {
        var replyTopicUrl = getReplyTopicUrl(replyChannel);
        var replyProducer = producersManager.getProducer(replyTopicUrl, producerBuilder -> {
        });
        var executionException = new CommandExecutionException(status);
        var value = serializeCommandExecutionException(executionException);
        replyProducer.newMessage()
                     .property(Headers.CORRELATION_ID, correlationId)
                     .property(USER_ID, userId)
                     .property(ERROR, value)
                     .sendAsync();
    }

    private void replyWithSuccessResponse(String replyChannel, String correlationId, String userId, R response) {
        try {
            var topicUrl = getReplyTopicUrl(replyChannel);
            var producer = producersManager.getProducer(topicUrl, producerBuilder -> {
            });
            var value = objectMapper.writeValueAsBytes(response);
            producer.newMessage()
                    .property(Headers.CORRELATION_ID, correlationId)
                    .property(USER_ID, userId)
                    .value(value)
                    .sendAsync();
        } catch (JsonProcessingException e) {
            replyWithErrorResponse(replyChannel, correlationId, userId, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private String getReplyTopicUrl(String replyChannel) {
        return tenant + "/" + PulsarNamespaces.COMMAND_REPLIES + "/" + replyChannel;
    }

    private String getSubscriptionName(CommandHandler<?, ?> handler) {
        return applicationName + "-" + handler.getChannelName() + "-handler";
    }

    private String getConsumerName(CommandHandler<?, ?> handler) {
        return applicationName + "-" + handler.getChannelName() + "-handler";
    }

    private String getRequestsTopicUrl(CommandHandler<?, ?> handler) {
        var channelName = handler.getChannelName();
        return tenant + "/" + PulsarNamespaces.COMMAND_REQUESTS + "/" + channelName;
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
}
