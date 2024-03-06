package edu.stanford.protege.webprotege.ipc.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.CommandExecutionException;
import edu.stanford.protege.webprotege.ipc.CommandExecutor;
import edu.stanford.protege.webprotege.ipc.ExecutionContext;
import edu.stanford.protege.webprotege.ipc.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;

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


    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    @Lazy
    private RabbitTemplate rabbitTemplate;

    public CommandExecutorImpl(Class<R> responseClass) {
        this.responseClass = responseClass;
    }

    @Override
    public CompletableFuture<R> execute(Q request, ExecutionContext executionContext) {
        try {
            var json = objectMapper.writeValueAsBytes(request);
            org.springframework.amqp.core.Message rabbitRequest = new org.springframework.amqp.core.Message(json);
            rabbitRequest.getMessageProperties().getHeaders().put(Headers.ACCESS_TOKEN, executionContext.jwt());
            rabbitRequest.getMessageProperties().getHeaders().put(Headers.USER_ID, executionContext.userId());
            rabbitRequest.getMessageProperties().getHeaders().put(Headers.METHOD, request.getChannel());
            org.springframework.amqp.core.Message rabbitResponse = rabbitTemplate.sendAndReceive(request.getChannel(), rabbitRequest);

            CompletableFuture<R> replyHandler = new CompletableFuture<>();

            assert rabbitResponse != null;
            var error = (String) rabbitResponse.getMessageProperties().getHeaders().get(Headers.ERROR);
            if (error != null) {
                var executionException = objectMapper.readValue(error, CommandExecutionException.class);
                replyHandler.completeExceptionally(executionException);
            }
            else {
                var response = objectMapper.readValue(rabbitResponse.getBody(), responseClass);
                replyHandler.complete(response);
            }
            return replyHandler;
        } catch (Exception e) {
            logger.error("Error ", e);
            throw new RuntimeException(e);
        }
    }

}
