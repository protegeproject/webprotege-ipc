package edu.stanford.protege.webprotege.ipc.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.UserId;
import edu.stanford.protege.webprotege.ipc.EventDispatchException;
import edu.stanford.protege.webprotege.ipc.ExecutionContext;
import edu.stanford.protege.webprotege.ipc.Headers;
import edu.stanford.protege.webprotege.ipc.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class RabbitMQEventDispatcherTest {

    private static final String APPLICATION_NAME = "test-app";

    private static final long CONFIRM_TIMEOUT = 5000L;

    private static final String PROJECT_ID = "the-project-id";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private RabbitTemplate eventRabbitTemplate;

    private RabbitMQEventDispatcher dispatcher;

    private TestEvent event;

    private ExecutionContext executionContext;

    @BeforeEach
    void setUp() {
        dispatcher = new RabbitMQEventDispatcher(APPLICATION_NAME, objectMapper, eventRabbitTemplate, CONFIRM_TIMEOUT);
        event = new TestEvent(UUID.randomUUID().toString(), PROJECT_ID);
        executionContext = new ExecutionContext(UserId.valueOf("the-user"), "the-jwt", UUID.randomUUID().toString());
        // Run the invoke callback against the same template so the send/confirm calls are exercised.
        when(eventRabbitTemplate.invoke(any())).thenAnswer(invocation -> {
            RabbitOperations.OperationsCallback<?> callback = invocation.getArgument(0);
            return callback.doInRabbit(eventRabbitTemplate);
        });
    }

    @Test
    void shouldPropagateAmqpExceptionWhenSendFails() {
        doThrow(new AmqpException("broker down"))
                .when(eventRabbitTemplate).convertAndSend(anyString(), anyString(), any(Message.class));
        assertThatThrownBy(() -> dispatcher.dispatchEvent(event, executionContext))
                .isInstanceOf(AmqpException.class);
    }

    @Test
    void shouldPropagateAmqpExceptionWhenBrokerDoesNotConfirm() {
        doThrow(new AmqpException("broker did not confirm"))
                .when(eventRabbitTemplate).waitForConfirmsOrDie(anyLong());
        assertThatThrownBy(() -> dispatcher.dispatchEvent(event, executionContext))
                .isInstanceOf(AmqpException.class);
    }

    @Test
    void shouldWrapJsonProcessingExceptionInEventDispatchException() {
        JsonProcessingException jsonError = new JsonProcessingException("cannot serialize") {};
        ObjectMapper failingMapper = new ObjectMapper() {
            @Override
            public byte[] writeValueAsBytes(Object value) throws JsonProcessingException {
                throw jsonError;
            }
        };
        RabbitMQEventDispatcher failingDispatcher =
                new RabbitMQEventDispatcher(APPLICATION_NAME, failingMapper, eventRabbitTemplate, CONFIRM_TIMEOUT);
        assertThatThrownBy(() -> failingDispatcher.dispatchEvent(event, executionContext))
                .isInstanceOf(EventDispatchException.class)
                .hasCause(jsonError);
    }

    @Test
    void shouldSetHeadersAndWaitForConfirmOnSuccess() {
        dispatcher.dispatchEvent(event, executionContext);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(eventRabbitTemplate).convertAndSend(eq(RabbitMQEventsConfiguration.EVENT_EXCHANGE), eq(""), messageCaptor.capture());
        verify(eventRabbitTemplate).waitForConfirmsOrDie(CONFIRM_TIMEOUT);

        var headers = messageCaptor.getValue().getMessageProperties().getHeaders();
        assertThat(headers.get(Headers.CHANNEL)).isEqualTo(TestEvent.CHANNEL);
        assertThat(headers.get(Headers.SERVICE_NAME)).isEqualTo(APPLICATION_NAME);
        assertThat(headers.get(Headers.EVENT_TYPE)).isEqualTo("TestEvent");
        assertThat(headers.get(Headers.PROJECT_ID)).isEqualTo(PROJECT_ID);
        assertThat(headers.get(Headers.ACCESS_TOKEN)).isEqualTo("the-jwt");
        assertThat(headers.get(Headers.USER_ID)).isEqualTo("the-user");
    }

    @Test
    void shouldWaitForConfirmWhenDispatchingWithoutExecutionContext() {
        dispatcher.dispatchEvent(event);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(eventRabbitTemplate).convertAndSend(eq(RabbitMQEventsConfiguration.EVENT_EXCHANGE), eq(""), messageCaptor.capture());
        verify(eventRabbitTemplate).waitForConfirmsOrDie(CONFIRM_TIMEOUT);

        var headers = messageCaptor.getValue().getMessageProperties().getHeaders();
        assertThat(headers.get(Headers.CHANNEL)).isEqualTo(TestEvent.CHANNEL);
        assertThat(headers).doesNotContainKey(Headers.ACCESS_TOKEN);
        assertThat(headers).doesNotContainKey(Headers.USER_ID);
    }
}
