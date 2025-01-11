package edu.stanford.protege.webprotege.ipc.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;


/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-01-31
 * <p>
 * A {@link CommandExecutorImpl} is used to execute a specific command that has a specific type of request and
 * a specific type of response.  That is, a given command executor instance only handles requests for single channel.
 */
public class CommandExecutorImpl<Q extends Request<R>, R extends Response> implements CommandExecutor<Q, R> {

    private static final Logger logger = LoggerFactory.getLogger(CommandExecutorImpl.class);

    private final Class<R> responseClass;


    private ObjectMapper objectMapper;

    private AsyncRabbitTemplate asyncRabbitTemplate;

    public CommandExecutorImpl(Class<R> responseClass) {
        this.responseClass = responseClass;
    }

    @Override
    public CompletableFuture<R> execute(Q request, ExecutionContext executionContext) {
        try {
            var json = objectMapper.writeValueAsBytes(request);
            var rabbitMsg = new Message(json);
            var headers = rabbitMsg.getMessageProperties().getHeaders();
            headers.put(Headers.ACCESS_TOKEN, executionContext.jwt());
            headers.put(Headers.USER_ID, executionContext.userId().id());
            headers.put(Headers.METHOD, request.getChannel());
            return asyncRabbitTemplate.sendAndReceive(request.getChannel(), rabbitMsg).thenApply(this::handleResponse);
        } catch (JsonProcessingException e) {
            var serializationException = new MessageBodySerializationException(request, e);
            logger.error("Error when serializing request", e);
            return CompletableFuture.failedFuture(serializationException);
        } catch (AmqpException e) {
            logger.error("Error thrown by message broker", e);
            return CompletableFuture.failedFuture(e);
        } catch (Exception e) {
            logger.error("Other error when executing request", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Handles a response message from Rabbit.  The rabbit messag is will either contain an error, stored
     * in the error header, or its body will contain a {@link Response} serialized as JSON.  Deserialization first
     * checks to see if there is an error header.  If there is, the error is deserialized as {@link CommandExecutionException}
     * and this is thrown.  If there is no error header then the message body is deserialized a {@link Response} and
     * this response is returned.
     * @param rabbitResponseMsg The message to handle
     * @return The message body deserialized as a {@link Response}
     * @throws CommandExecutionException if the message has an error header.  The error header is deserialized as a
     * {@link CommandExecutionException} and thrown
     * @throws MessageErrorHeaderDeserializationException if the message has an error header and there was a problem
     * deserializing the error header.
     * @throws MessageBodyDeserializationException if there was a problem deserializing the message body.
     */
    private R handleResponse(Message rabbitResponseMsg) {
        var errorHeader = (String) rabbitResponseMsg.getMessageProperties().getHeaders().get(Headers.ERROR);
        if(errorHeader != null) {
            try {
                logger.info("Found error on response {}. Action : {}" ,errorHeader, rabbitResponseMsg.getMessageProperties().getHeaders().get(Headers.METHOD));
                throw objectMapper.readValue(errorHeader, CommandExecutionException.class);
            } catch (JsonProcessingException e) {
                logger.error("Error deserializing CommandExecutionException. Error header: {}. Exception {}", errorHeader, e.getMessage(), e);
                throw new MessageErrorHeaderDeserializationException(errorHeader, e);
            }
        } else {
            try {
                return objectMapper.readValue(rabbitResponseMsg.getBody(), responseClass);
            } catch (IOException e) {
                logger.error("Error deserializing reply message. Body: {} Cause: {}", rabbitResponseMsg.getBody(), e.getMessage(), e);
                throw new MessageBodyDeserializationException(rabbitResponseMsg.getBody(), e);
            }
        }
    }

    @Autowired
    @Qualifier("asyncRabbitTemplate")
    @Lazy
    public void setAsyncRabbitTemplate(AsyncRabbitTemplate asyncRabbitTemplate) {
        this.asyncRabbitTemplate = asyncRabbitTemplate;
    }

    @Autowired
    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }
}
