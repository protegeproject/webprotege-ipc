package edu.stanford.protege.webprotege.ipc.impl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.CommandExecutionException;
import edu.stanford.protege.webprotege.ipc.CommandExecutor;
import edu.stanford.protege.webprotege.ipc.CommandHandler;
import edu.stanford.protege.webprotege.ipc.Headers;
import edu.stanford.protege.webprotege.ipc.util.CorrelationMDCUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class RabbitMqCommandHandlerWrapperTest {

    @Mock
    private CommandHandler<Request<Response>, Response> mockHandler;

    @Mock
    private CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> mockAuthExecutor;

    @Mock
    private Channel mockChannel;

    private RabbitMqCommandHandlerWrapper<Request<Response>, Response> wrapper;

    @Mock
    private Request<Response> request;

    @Mock
    private Response response;

    @Mock
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() throws Exception {
        wrapper = new RabbitMqCommandHandlerWrapper<>("test-app", 
                                                    Collections.singletonList(mockHandler), 
                                                    objectMapper, 
                                                    mockAuthExecutor);
        when(mockHandler.getChannelName()).thenReturn("test-channel");
        
        // Setup ObjectMapper mocks for common operations
        when(mockHandler.getRequestClass()).thenReturn((Class) Request.class);
        
        // Mock readValue for deserializing requests
        when(objectMapper.readValue(any(byte[].class), eq(Request.class))).thenReturn(request);
        
        // Mock writeValueAsBytes for serializing responses
        when(objectMapper.writeValueAsBytes(any(Response.class))).thenReturn("test-response".getBytes());
        
        // Mock writeValueAsString for serializing exceptions
        when(objectMapper.writeValueAsString(any(CommandExecutionException.class))).thenReturn("test-exception");
    }

    @Test
    void shouldSetAndClearCorrelationIdWhenProcessingValidMessage() throws Exception {
        // Given
        String correlationId = UUID.randomUUID().toString();
        Message message = createValidMessage(correlationId);
        when(mockHandler.handleRequest(eq(request), any())).thenReturn(Mono.just(response));
        // ObjectMapper is already mocked in setUp() - no need to mock again

        // When
        wrapper.onMessage(message, mockChannel);
        
        // Then
        // Correlation ID should be cleared after processing
        assertThat(CorrelationMDCUtil.getCorrelationId()).isNull();
    }

    @Test
    void shouldReplyWithBadRequestWhenProcessingMessageWithoutCorrelationId() throws Exception {
        // Given
        Message message = createMessageWithoutCorrelationId();
        
        // When
        assertThatThrownBy(() ->  wrapper.onMessage(message, mockChannel))
                .isInstanceOf(CommandExecutionException.class);

        // Then
        // Verify that replyWithBadRequest was called with the correct parameters
        verify(mockChannel).basicPublish(
            eq(RabbitMqConfiguration.COMMANDS_EXCHANGE),
            eq(message.getMessageProperties().getReplyTo()),
            any(),
            any(byte[].class)
        );
        
        // Correlation ID should not be set
        assertThat(CorrelationMDCUtil.getCorrelationId()).isNull();
        verify(mockHandler, never()).handleRequest(any(), any());
    }

    private Message createValidMessage(String correlationId) {
        var headers = new HashMap<String, Object>();
        headers.put(Headers.CORRELATION_ID, correlationId);
        headers.put(Headers.REPLY_CHANNEL, "reply-channel");
        headers.put(Headers.USER_ID, "test-user");
        headers.put(Headers.ACCESS_TOKEN, "test-token");
        headers.put(Headers.METHOD, "test-channel");

        return MessageBuilder.withBody(new byte[0])
                .copyHeaders(headers)
                .setReplyTo("reply-channel")
                .build();
    }

    private Message createMessageWithoutCorrelationId() {
        var headers = new HashMap<String, Object>();
        headers.put(Headers.REPLY_CHANNEL, "reply-channel");
        headers.put(Headers.USER_ID, "test-user");
        headers.put(Headers.ACCESS_TOKEN, "test-token");
        headers.put(Headers.METHOD, "test-channel");

        return MessageBuilder.withBody(new byte[0])
                .copyHeaders(headers)
                .build();
    }
} 