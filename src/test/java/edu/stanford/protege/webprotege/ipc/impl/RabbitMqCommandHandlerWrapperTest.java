package edu.stanford.protege.webprotege.ipc.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.ipc.CommandExecutor;
import edu.stanford.protege.webprotege.ipc.CommandHandler;
import edu.stanford.protege.webprotege.ipc.Headers;
import edu.stanford.protege.webprotege.ipc.util.CorrelationMDCUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RabbitMqCommandHandlerWrapperTest {

    @Mock
    private CommandHandler<Request<Response>, Response> mockHandler;

    @Mock
    private CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> mockAuthExecutor;

    @Mock
    private Channel mockChannel;

    private RabbitMqCommandHandlerWrapper<Request<Response>, Response> wrapper;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();

        wrapper = new RabbitMqCommandHandlerWrapper<>("test-app", 
                                                    Collections.singletonList(mockHandler), 
                                                    objectMapper, 
                                                    mockAuthExecutor);
    }

    @Test
    void shouldSetAndClearCorrelationIdWhenProcessingValidMessage() throws Exception {
        // Given
        String correlationId = UUID.randomUUID().toString();
        Message message = createValidMessage(correlationId);
        
        // When
        wrapper.onMessage(message, mockChannel);
        
        // Then
        // Correlation ID should be cleared after processing
        assertThat(CorrelationMDCUtil.getCorrelationId()).isNull();
        verify(mockHandler, never()).handleRequest(any(), any());
    }

    @Test
    void shouldReplyWithBadRequestWhenProcessingMessageWithoutCorrelationId() throws Exception {
        // Given
        Message message = createMessageWithoutCorrelationId();
        
        // When
        wrapper.onMessage(message, mockChannel);
        
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