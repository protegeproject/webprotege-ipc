package edu.stanford.protege.webprotege.ipc.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static edu.stanford.protege.webprotege.ipc.Headers.*;
import static edu.stanford.protege.webprotege.ipc.impl.RabbitMqConfiguration.COMMANDS_EXCHANGE;

public class RabbitMqCommandHandlerWrapper<Q extends Request<R>, R extends Response> implements ChannelAwareMessageListener {

    private final static Logger logger = LoggerFactory.getLogger(RabbitMqCommandHandlerWrapper.class);

    private final List<CommandHandler<? extends Request, ? extends Response>> handlers;


    private final ObjectMapper objectMapper;

    private final CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor;

    public RabbitMqCommandHandlerWrapper(List<CommandHandler<? extends Request, ? extends Response>> handlers, ObjectMapper objectMapper, CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor) {
        this.handlers = handlers;
        this.objectMapper = objectMapper;
        this.authorizationStatusExecutor = authorizationStatusExecutor;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws Exception {
        logger.info("Received message " + message);

        var replyChannel = message.getMessageProperties().getReplyTo();
        if (replyChannel == null) {
            String errorMessage = Headers.REPLY_CHANNEL + " header is missing.  Cannot reply to message.";
            replyWithValidationError(message, channel, errorMessage);

            return;
        }

        var correlationId = message.getMessageProperties().getCorrelationId();
        if (correlationId == null) {
            String errorMessage = Headers.CORRELATION_ID + " header is missing.  Cannot process message.";
            replyWithValidationError(message, channel, errorMessage);
            return;
        }

        var userId = (String) message.getMessageProperties().getHeaders().get(USER_ID);
        if (userId == null) {
            String errorMessage = USER_ID + " header is missing.  Cannot process message.  Returning Forbidden Error Code.  Message reply topic: "+replyChannel;
            replyWithValidationError(message, channel, errorMessage);
            return;
        }

        var accessToken = String.valueOf(message.getMessageProperties().getHeaders().get(ACCESS_TOKEN));
        if(accessToken == null) {
            String errorMessage = ACCESS_TOKEN + " header is missing.  Cannot process message.  Returning Forbidden Error Code.  Message reply topic: " + replyChannel;
            replyWithValidationError(message, channel, errorMessage);
            return;
        }

        var messageType = (String) message.getMessageProperties().getHeaders().get(METHOD);
        if(messageType == null) {

            String errorMessage = METHOD + " header is missing.  Cannot process message.  Returning Forbidden Error Code.  Message reply topic: " + replyChannel;
            replyWithValidationError(message, channel, errorMessage);
            return;
        }

        CommandHandler handler = extractHandler(messageType);
        logger.info("Dispatch handling to {}", handler.getClass());
        parseAndHandleRequest(handler, message, channel, new UserId(userId), accessToken);
    }

    private void replyWithValidationError(Message message, Channel channel, String errorMessage) throws IOException, TimeoutException {

        AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                .Builder()
                .correlationId(message.getMessageProperties().getCorrelationId())
                .build();

        logger.error(errorMessage);
        channel.basicPublish("", message.getMessageProperties().getReplyTo(), replyProps, errorMessage.getBytes());
    }

    private void parseAndHandleRequest(CommandHandler<Q,R> handler, Message message, Channel channel, UserId userId, String accessToken) {
        try {
            var request = objectMapper.readValue( message.getBody(), handler.getRequestClass());
            // The request has successfully been read.  All required headers are present and the request body
            // is well-formed so acknowledge the request (i.e. it shouldn't be dead-lettered)
            if (handler instanceof AuthorizedCommandHandler<Q, R> authorizedCommandHandler) {
                authorizeAndReplyToRequest(handler, message, channel, userId, request, authorizedCommandHandler, accessToken);
            }
            else {
                handleAndReplyToRequest(handler, channel, message, userId, request, accessToken);
            }

        } catch (IOException e) {
            logger.error("Could not parse request", e);
            replyWithErrorResponse(message, channel, userId, HttpStatus.BAD_REQUEST);
        }
    }

    private CommandHandler<? extends Request, ? extends Response> extractHandler(String messageType){
        return this.handlers.stream().filter(handler -> {
                    return handler.getChannelName().equalsIgnoreCase(messageType);
                } )
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Invalid message type " + messageType));
    }

    private void authorizeAndReplyToRequest(CommandHandler<Q,R> handler,
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
        var executionContext = new ExecutionContext(userId, "");
        var authResponseFuture = authorizationStatusExecutor.execute(authRequest, executionContext);
        authResponseFuture.whenComplete((authResponse, authError) -> {
            if (authError != null) {
                // The call to the authorization service failed
                logger.warn("An error occurred when requesting the authorization status for {} on {}. Error: {}",
                        userId,
                        resource,
                        authError.getMessage());
                // Upstream Error
                replyWithErrorResponse(message, channel, userId, HttpStatus.INTERNAL_SERVER_ERROR);
            }
            else {
                // The call to the authorization service succeeded
                if (authResponse.authorizationStatus() == AuthorizationStatus.AUTHORIZED) {
                    handleAndReplyToRequest(handler, channel, message, userId, request, accessToken);
                }
                else {
                    logger.info("Permission denied when attempting to execute a request.  User: {}, Request: {}",
                            userId,
                            request);
                    replyWithErrorResponse(message, channel, userId, HttpStatus.FORBIDDEN);
                }
            }

        });
    }

    private void handleAndReplyToRequest(CommandHandler<Q,R> handler, Channel channel, Message message, UserId userId, Q request, String accessToken) {
        var executionContext = new ExecutionContext(userId, accessToken);
        try {
            var response = handler.handleRequest(request, executionContext);
            response.subscribe(r -> {
                replyWithSuccessResponse(channel, message, userId, r);
            }, throwable -> {
                if (throwable instanceof CommandExecutionException ex) {
                    logger.info(
                            "The command handler threw a CommandExecutionException exception while handling a request.  Sending an error as the reply.  Code: {}, Message: {},  Request: {}",
                            ex.getStatusCode(),
                            throwable.getMessage(),
                            request);
                    replyWithErrorResponse(message,channel, userId, ex.getStatus());
                }
                else {
                    replyWithErrorResponse(message, channel, userId, HttpStatus.INTERNAL_SERVER_ERROR);
                }
            });
        } catch (Throwable throwable) {
            logger.error("Uncaught exception when handling request", throwable);
            replyWithErrorResponse(message, channel, userId, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void replyWithErrorResponse(Message message, Channel channel, UserId userId, HttpStatus status) {
        try {

            var executionException = new CommandExecutionException(status);
            var value = serializeCommandExecutionException(executionException);
            Map<String, Object> headersMap = new HashMap<>();
            headersMap.put(ERROR, String.valueOf(value));
            headersMap.put(USER_ID, String.valueOf(userId.id()));
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(message.getMessageProperties().getCorrelationId())
                    .headers(headersMap)
                    .build();


            channel.basicPublish(COMMANDS_EXCHANGE, message.getMessageProperties().getReplyTo(), replyProps, value.getBytes());
        } catch (Exception e){
            logger.error("Error replyWithErrorResponse ", e);
        }
    }

    private void replyWithSuccessResponse(Channel channel, Message message, UserId userId, R response) {
        try {
            var value = objectMapper.writeValueAsBytes(response);

            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(message.getMessageProperties().getCorrelationId())
                    .build();

            channel.basicPublish(COMMANDS_EXCHANGE, message.getMessageProperties().getReplyTo(), replyProps, value);
        } catch (Exception e) {
            logger.error("Error handling replyWithSuccessResponse ", e);
            replyWithErrorResponse(message, channel, userId, HttpStatus.INTERNAL_SERVER_ERROR);
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
}
