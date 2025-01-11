package edu.stanford.protege.webprotege.ipc.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import edu.stanford.protege.webprotege.authorization.AuthorizationStatus;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.authorization.Subject;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.common.UserId;
import edu.stanford.protege.webprotege.ipc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static edu.stanford.protege.webprotege.ipc.Headers.*;
import static edu.stanford.protege.webprotege.ipc.impl.RabbitMqConfiguration.COMMANDS_EXCHANGE;

public class RabbitMqCommandHandlerWrapper<Q extends Request<R>, R extends Response> implements ChannelAwareMessageListener {

    private final static Logger logger = LoggerFactory.getLogger(RabbitMqCommandHandlerWrapper.class);

    private final List<CommandHandler<? extends Request, ? extends Response>> handlers;

    private final String applicationName;

    private final ObjectMapper objectMapper;

    private final CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor;

    public RabbitMqCommandHandlerWrapper(@Value("${spring.application.name}") String applicationName, List<CommandHandler<? extends Request, ? extends Response>> handlers, ObjectMapper objectMapper, CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor) {
        this.applicationName = applicationName;
        this.handlers = handlers;
        this.objectMapper = objectMapper;
        this.authorizationStatusExecutor = authorizationStatusExecutor;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws Exception {
        var replyChannel = message.getMessageProperties().getReplyTo();
        if (replyChannel == null) {
            var errorMessage = Headers.REPLY_CHANNEL + " header is missing.  Cannot reply to message.";
            replyWithBadRequest(message, channel, errorMessage);
            return;
        }

        var correlationId = message.getMessageProperties().getCorrelationId();
        if (correlationId == null) {
            var errorMessage = Headers.CORRELATION_ID + " header is missing.  Cannot process message.";
            replyWithBadRequest(message, channel, errorMessage);
            return;
        }

        var userId = (String) message.getMessageProperties().getHeaders().get(USER_ID);
        if (userId == null) {
            var errorMessage = USER_ID + " header is missing.  Cannot process message.  Message reply topic: " + replyChannel;
            replyWithBadRequest(message, channel, errorMessage);
            return;
        }

        var accessToken = String.valueOf(message.getMessageProperties().getHeaders().get(ACCESS_TOKEN));
        if (accessToken == null) {
            var errorMessage = ACCESS_TOKEN + " header is missing.  Cannot process message.  Message reply topic: " + replyChannel;
            replyWithBadRequest(message, channel, errorMessage);
            return;
        }

        var messageType = (String) message.getMessageProperties().getHeaders().get(METHOD);
        if (messageType == null) {
            var errorMessage = METHOD + " header is missing.  Cannot process message.  Message reply topic: " + replyChannel;
            replyWithBadRequest(message, channel, errorMessage);
            return;
        }

        var handler = extractHandler(messageType);
        if (handler.isEmpty()) {
            logger.warn("Command handler for message not found.  Message type: {}", messageType);
            var errorMessage = "Cannot find command handler for messages type " + messageType;
            var ex = new CommandExecutionException(HttpStatus.INTERNAL_SERVER_ERROR, "", errorMessage);
            replyWithErrorResponse(message, channel, UserId.valueOf(userId), ex);
        } else {
            //noinspection unchecked
            parseAndHandleRequest(handler.get(), message, channel, UserId.valueOf(userId), accessToken);
        }
    }

    private void replyWithBadRequest(Message message, Channel channel, String errorMessage) {
        logger.error("Replying to message with 400 (BAD REQUEST): {}", errorMessage);
        replyWithErrorResponse(message, channel, null, CommandExecutionException.of(HttpStatus.BAD_REQUEST, errorMessage));
    }

    private void parseAndHandleRequest(CommandHandler<Q, R> handler, Message message, Channel channel, UserId userId, String accessToken) {
        try {
            var request = objectMapper.readValue(message.getBody(), handler.getRequestClass());
            // The request has successfully been read.  All required headers are present and the request body
            // is well-formed so acknowledge the request (i.e. it shouldn't be dead-lettered)
            if (handler instanceof AuthorizedCommandHandler<Q, R> authorizedCommandHandler) {
                authorizeAndReplyToRequest(handler, message, channel, userId, request, authorizedCommandHandler, accessToken);
            } else {
                handleAndReplyToRequest(handler, channel, message, userId, request, accessToken);
            }
        }
        catch (DatabindException | StreamReadException e) {
            logger.error("Could not parse request.  Request: {}.  Error: {}", message.getBody(), e.getMessage(), e);
            var msg = "Could not parse request: " + e.getMessage();
            replyWithErrorResponse(message, channel, userId,
                    CommandExecutionException.of(e, HttpStatus.BAD_REQUEST, msg));
        }
        catch (IOException e) {
            logger.error("Could not read message.  Request: {}.  Error: {}", message.getBody(), e.getMessage(), e);
            var msg = "Could not read request message: " + e.getMessage();
            replyWithErrorResponse(message, channel, userId,
                    CommandExecutionException.of(e, HttpStatus.INTERNAL_SERVER_ERROR, msg));
        }
    }

    @SuppressWarnings("rawtypes")
    private Optional<CommandHandler> extractHandler(String messageType) {
        return this.handlers.stream().filter(handler -> {
                    return handler.getChannelName().equalsIgnoreCase(messageType);
                })
                .map(h -> (CommandHandler) h)
                .findFirst();
    }

    private void authorizeAndReplyToRequest(CommandHandler<Q, R> handler,
                                            Message message,
                                            Channel channel,
                                            UserId userId,
                                            Q request,
                                            AuthorizedCommandHandler<Q, R> authenticatingCommandHandler,
                                            String accessToken) {
        var resource = authenticatingCommandHandler.getTargetResource(request);
        var subject = Subject.forUser(userId);
        var requiredActionId = authenticatingCommandHandler.getRequiredCapabilities();
        // Call to the authorization service to check
        var authRequest = new GetAuthorizationStatusRequest(resource,
                subject,
                requiredActionId.stream().findFirst().orElse(null));
        var executionContext = new ExecutionContext(userId, accessToken);
        var authResponseFuture = authorizationStatusExecutor.execute(authRequest, executionContext);
        authResponseFuture.whenComplete((authResponse, authError) -> {
            if (authError != null) {
                // The call to the authorization service failed
                logger.warn("Error requesting the authorization status for {} on {}. Error: {}",
                        userId,
                        resource,
                        authError.getMessage());
                replyWithErrorResponse(message, channel, userId, CommandExecutionException.of(authError));
            } else {
                // The call to the authorization service succeeded
                if (authResponse.authorizationStatus() == AuthorizationStatus.AUTHORIZED) {
                    handleAndReplyToRequest(handler, channel, message, userId, request, accessToken);
                } else {
                    logger.info("Permission denied when attempting to execute a request.  User: {}, Request: {}",
                            userId,
                            request);
                    var msg = "Permission denied for " + request + " on " + resource;
                    replyWithErrorResponse(message, channel, userId, CommandExecutionException.of(HttpStatus.FORBIDDEN, msg));
                }
            }

        });
    }

    private void handleAndReplyToRequest(CommandHandler<Q, R> handler, Channel channel, Message message, UserId userId, Q request, String accessToken) {
        var executionContext = new ExecutionContext(userId, accessToken);
        var startTime = System.currentTimeMillis();
        try {
            var response = handler.handleRequest(request, executionContext);
            response.subscribe(r -> {
                var endTime = System.currentTimeMillis();
                logger.info("Request executed {}. Time taken for Execution is : {}ms", request.getChannel(), endTime - startTime);
                replyWithSuccessResponse(channel, message, userId, r);
            }, throwable -> {
                var endTime = System.currentTimeMillis();
                var ex = CommandExecutionException.of(throwable);
                logger.info("Request failed {} with error {}. Time taken for Execution is : {}ms", request.getChannel(), throwable.getMessage(), endTime - startTime);
                replyWithErrorResponse(message, channel, userId, ex);
            });
        } catch (Throwable throwable) {
            // Catch any exception that had leaked out of the handleRequest method
            var endTime = System.currentTimeMillis();
            logger.info("Request failed {} with error {}. Time taken for Execution is : {}ms", request.getChannel(), throwable.getMessage(), endTime - startTime);
            replyWithErrorResponse(message, channel, userId, CommandExecutionException.of(throwable));
        }
    }

    private void replyWithErrorResponse(Message message, Channel channel, @Nullable UserId userId, CommandExecutionException executionException) {
        try {
            var value = serializeCommandExecutionException(executionException);
            var headersMap = new HashMap<String, Object>();
            headersMap.put(ERROR, String.valueOf(value));
            if (userId != null) {
                headersMap.put(USER_ID, String.valueOf(userId.id()));
            }
            headersMap.put(SERVICE_NAME, applicationName);
            var replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(message.getMessageProperties().getCorrelationId())
                    .headers(headersMap)
                    .build();
            channel.basicPublish(COMMANDS_EXCHANGE, message.getMessageProperties().getReplyTo(), replyProps, value.getBytes());
        } catch (Exception e) {
            logger.error("Error publsihing reponse ", e);
        }
    }

    private void replyWithSuccessResponse(Channel channel, Message message, UserId userId, R response) {
        try {
            var value = objectMapper.writeValueAsBytes(response);
            var headersMap = new HashMap<String, Object>();
            headersMap.put(SERVICE_NAME, applicationName);
            var replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(message.getMessageProperties().getCorrelationId())
                    .headers(headersMap)
                    .build();

            channel.basicPublish(COMMANDS_EXCHANGE, message.getMessageProperties().getReplyTo(), replyProps, value);
        } catch (JsonProcessingException e) {
            logger.error("Error serializing response.  Response: {}.  Error: {}", response, e.getMessage(), e);
            var msg = "Could not serialize response: " + e.getMessage();
            replyWithErrorResponse(message, channel, userId,
                    CommandExecutionException.of(e, HttpStatus.INTERNAL_SERVER_ERROR, msg));
        }
        catch (IOException e) {
            logger.error("Error creating and sending response.  Response: {}.  Error: {}", response, e.getMessage(), e);
            var msg = "Error creating and sending response: " + e.getMessage();
            replyWithErrorResponse(message, channel, userId,
                    CommandExecutionException.of(e, HttpStatus.INTERNAL_SERVER_ERROR, msg));
        }
        catch (Throwable e) {
            logger.error("Error handling replyWithSuccessResponse ", e);
            replyWithErrorResponse(message, channel, userId, CommandExecutionException.of(e));
        }
    }

    private String serializeCommandExecutionException(CommandExecutionException exception) {
        try {
            return objectMapper.writeValueAsString(exception);
        } catch (JsonProcessingException e) {
            logger.error("Error while serializing CommandExecutionException", e);
            return """
                    {
                        "statusCode" : 500,
                        "causeMessage" : "%s",
                        "causeClassName" "com.fasterxml.jackson.core.JsonProcessingException"
                    }
                    """.formatted(e.getMessage()).strip();
        }

    }
}
