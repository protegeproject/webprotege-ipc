package edu.stanford.protege.webprotege.ipc.pulsar;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
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
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.List;

import static edu.stanford.protege.webprotege.ipc.Headers.*;
import static edu.stanford.protege.webprotege.ipc.pulsar.PulsarCommandHandlersConfiguration.RPC_EXCHANGE;

public class RabbitMqHandlerWrapper <Q extends Request<R>, R extends Response> implements ChannelAwareMessageListener {

    private final static Logger logger = LoggerFactory.getLogger(RabbitMqHandlerWrapper.class);

    private final List<CommandHandler<? extends Request, ? extends Response>> handlers;


    private final ObjectMapper objectMapper;
    private final CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor;

    private final RabbitTemplate rabbitTemplate;
    public RabbitMqHandlerWrapper(List<CommandHandler<? extends Request, ? extends Response>> commandHandlers, ObjectMapper objectMapper, CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor, RabbitTemplate rabbitTemplate) {
        this.handlers = commandHandlers;
        this.objectMapper = objectMapper;
        this.authorizationStatusExecutor = authorizationStatusExecutor;
        this.rabbitTemplate = rabbitTemplate;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws Exception {
        logger.info("Received message " + message);


        var replyChannel = message.getMessageProperties().getReplyTo();
        if (replyChannel == null) {
            logger.error(Headers.REPLY_CHANNEL + " header is missing.  Cannot reply to message.");
            channel.basicPublish("",message.getMessageProperties().getReplyTo(), null, (Headers.REPLY_CHANNEL + " header is missing.  Cannot reply to message.").getBytes());
            return;
        }

        var correlationId = message.getMessageProperties().getCorrelationId();
        if (correlationId == null) {
            logger.error(Headers.CORRELATION_ID + " header is missing.  Cannot process message.");
            channel.basicPublish("",message.getMessageProperties().getReplyTo(), null, (Headers.CORRELATION_ID + " header is missing.  Cannot process message.").getBytes());
            return;
        }

        var userId = (String) message.getMessageProperties().getHeaders().get(USER_ID);
        if (userId == null) {
            logger.error(USER_ID + " header is missing.  Cannot process message.  Returning Forbidden Error Code.  Message reply topic: {}",
                    replyChannel);
            channel.basicPublish("",message.getMessageProperties().getReplyTo(), null, (Headers.USER_ID + " header is missing.  Cannot process message.").getBytes());
            return;
        }

        var accessToken = String.valueOf(message.getMessageProperties().getHeaders().get(ACCESS_TOKEN));
        if(accessToken == null) {
            logger.error(ACCESS_TOKEN + " header is missing.  Cannot process message.  Returning Forbidden Error Code.  Message reply topic: {}",
                    replyChannel);
            channel.basicPublish("",message.getMessageProperties().getReplyTo(), null, (Headers.ACCESS_TOKEN + " header is missing.  Cannot process message.").getBytes());
            return;
        }

        var messageType = (String) message.getMessageProperties().getHeaders().get("webprotege_methodName");
        if(messageType == null) {
            logger.error("Method name header is missing.  Cannot process message.  Returning Forbidden Error Code.  Message reply topic: {}",
                    replyChannel);
            channel.basicPublish("",message.getMessageProperties().getReplyTo(), null, ("Method name header is missing.  Cannot process message.").getBytes());
            return;
        }
        //        var accessToken = inboundHeaders.lastHeader(Headers.ACCESS_TOKEN);
        //        if (accessToken == null) {
        //            logger.error(Headers.ACCESS_TOKEN + " header is missing.  Cannot process message.  Treating as unauthorized.  Message reply topic: {}", new String(replyTopicHeader.value(), StandardCharsets.UTF_8));
        //            sendError(record, replyTopicHeader, replyHeaders, HttpStatus.UNAUTHORIZED);
        //            return;
        //        }
        //        replyHeaders.add(new RecordHeader(Headers.ACCESS_TOKEN, accessToken.value()));

        CommandHandler handler = extractHandler(messageType);
        parseAndHandleRequest(handler, message, channel, new UserId(userId), accessToken);
    }

    private void parseAndHandleRequest(CommandHandler<Q,R> handler, Message message, Channel channel, UserId userId, String accessToken) {
        try {
            var payload = message.getBody();
            var request = objectMapper.readValue(payload, handler.getRequestClass());
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
        return this.handlers.stream().filter(handler -> handler.getChannelName().equalsIgnoreCase(messageType))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Invalid message type" + messageType));
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
        var authResponseFuture = authorizationStatusExecutor.executeRabbit(authRequest, executionContext);
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

            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(message.getMessageProperties().getCorrelationId())
                    .build();

            replyProps.getHeaders().put(ERROR, value);
            replyProps.getHeaders().put(USER_ID, userId);
            channel.basicPublish(RPC_EXCHANGE, message.getMessageProperties().getReplyTo(), replyProps, value.getBytes());
        } catch (Exception e){
            logger.error("Am erroare ", e);
        }
    }

    private void replyWithSuccessResponse(Channel channel, Message message, UserId userId, R response) {
        try {

            var value = objectMapper.writeValueAsBytes(response);
            logger.info("ALEX reply pe topic {} cu {}",message.getMessageProperties().getReplyTo(), objectMapper.writeValueAsString(response));
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(message.getMessageProperties().getCorrelationId())
                    .build();
            channel.basicPublish(RPC_EXCHANGE, message.getMessageProperties().getReplyTo(), replyProps, value);

        } catch (JsonProcessingException e) {
            logger.error("Am erroare ", e);
            replyWithErrorResponse(message, channel, userId, HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e){
            logger.error("Am erroare ", e);
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
