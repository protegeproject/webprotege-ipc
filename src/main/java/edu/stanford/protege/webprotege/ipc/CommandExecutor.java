package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;

import java.util.concurrent.CompletableFuture;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-03
 */
public interface CommandExecutor<Q extends Request<R>, R extends Response> {


    /**
     * Executes the given request asynchronously within the provided {@link ExecutionContext}.  This uses asynchronous
     * messaging under the hood.
     *
     * <p>
     * This method serializes the request object into a JSON payload, prepares the necessary message headers using
     * information from the execution context, and sends the request to the appropriate messaging channel as determined
     * by the request. The method then processes the response received from the reply messaging channel.
     * </p>
     *
     * <p>
     * If the response contains an error, signified by the presence of the "error" message header then the error is
     * deserialized as a {@link CommandExecutionException} and this deserialized {@link CommandExecutionException} is
     * propagated via the returned failed future.
     * Otherwise, the response payload is deserialized into the expected response type and is returned as a completed
     * future.
     * </p>
     *
     * @param request The request to be executed. Must not be {@code null}.
     * @param executionContext The execution context providing additional metadata such as user ID and JWT.
     *                         Must not be {@code null}.
     * @return A {@link CompletableFuture} that resolves to the response of type {@code R}, if successful.  On failure
     *  the exceptions returned by getCause() can be {@link CommandExecutionException},
     *  {@link org.springframework.amqp.AmqpException}, {@link MessageProcessingException}.  A {@link CommandExecutionException}
     *  describes an error that happened during handling of the request.  {@link org.springframework.amqp.AmqpException}
     *  indicated a problem with the messaging framework.  A {@link MessageProcessingException} indicates an error
     *  serializing or deserializing requests, responses or error headers.
     *
     *
     * <h2>Example Usage</h2>
     * <pre>
     * {@code
     * CommandExecutorImpl<MyRequest, MyResponse> executor = new CommandExecutorImpl<>(MyResponse.class);
     *
     * CompletableFuture<MyResponse> responseFuture = executor.execute(myRequest, myExecutionContext);
     *
     * responseFuture.thenAccept(response -> {
     *     // Process the successful response
     *     System.out.println("Received response: " + response);
     * }).exceptionally(throwable -> {
     *     // Handle errors
     *     if (throwable.getCause() instanceof CommandExecutionException) {
     *         CommandExecutionException exception = (CommandExecutionException) throwable.getCause();
     *         System.err.println("Command execution failed: " + throwable.getMessage());
     *     } else if(throwable.getCause() instanceof AmqpException) {
     *         System.err.println("AMQP error: " + throwable.getMessage();
     *     } else if(throwable.getCause() instance of MessageProcessingException) {
     *         System.err.println("Message processing error: " + throwable.getMessage();
     *     } else {
     *         System.err.println("An unexpected error occurred: " + throwable.getMessage());
     *     }
     *     return null; // Returning null as this is an error handler
     * });
     * }
     * </pre>
     *
     * @see Request
     * @see Response
     * @see ExecutionContext
     * @see Headers
     */
    CompletableFuture<R> execute(Q request, ExecutionContext executionContext);
}
