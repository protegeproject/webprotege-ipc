package edu.stanford.protege.webprotege.ipc.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.CommandExecutionException;
import edu.stanford.protege.webprotege.ipc.CommandExecutor;
import edu.stanford.protege.webprotege.ipc.ExecutionContext;
import edu.stanford.protege.webprotege.ipc.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
            org.springframework.amqp.core.Message rabbitRequest = new org.springframework.amqp.core.Message(json);
            rabbitRequest.getMessageProperties().getHeaders().put(Headers.ACCESS_TOKEN, executionContext.jwt());
            rabbitRequest.getMessageProperties().getHeaders().put(Headers.USER_ID, executionContext.userId().id());
            rabbitRequest.getMessageProperties().getHeaders().put(Headers.METHOD, request.getChannel());

            return asyncRabbitTemplate.sendAndReceive(request.getChannel(), rabbitRequest).thenApply(this::handleResponse);
        } catch (Exception e) {
            logger.error("Error ", e);
            throw new RuntimeException(e);
        }
    }


    private R handleResponse(Message rabbitResponse) {
        var exception = (String) rabbitResponse.getMessageProperties().getHeaders().get(Headers.ERROR);

        if(exception != null) {
            try {
                logger.error("Found error on response {}. Action : {}" ,exception, rabbitResponse.getMessageProperties().getHeaders().get(Headers.METHOD));
                throw objectMapper.readValue(exception, CommandExecutionException.class);
            } catch (JsonProcessingException e) {
                logger.error("Error ", e);
                throw new RuntimeException(e);
            }
        } else {
            try {
                return objectMapper.readValue(rabbitResponse.getBody(), responseClass);
            } catch (IOException e) {
                logger.error("Error ", e);

                throw new RuntimeException(e);
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
