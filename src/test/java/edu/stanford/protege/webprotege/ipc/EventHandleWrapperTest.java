package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.UserId;
import edu.stanford.protege.webprotege.ipc.impl.RabbitMQEventHandlerWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EventHandleWrapperTest {

    private RabbitMQEventHandlerWrapper eventHandler;

    @Mock
    private EventHandler dummyEventHandler;

    @BeforeEach
    public void setUp() {
        when(dummyEventHandler.getChannelName()).thenReturn("dummyChannelName");
        when(dummyEventHandler.getEventClass()).thenReturn(TestEvent.class);
        eventHandler = new RabbitMQEventHandlerWrapper(Arrays.asList(dummyEventHandler), new ObjectMapper());
    }


    @Test
    public void GIVEN_eventOnDummyChannel_WHEN_handleEvent_THEN_correctHandlerIsCalled() throws JsonProcessingException {
        TestEvent testEvent = new TestEvent("1", "2");
        Message message = MessageBuilder.withBody(new ObjectMapper().writeValueAsBytes(testEvent)).build();
        message.getMessageProperties().setHeaders(new HashMap<>());
        message.getMessageProperties().getHeaders().put(Headers.CHANNEL, "dummyChannelName");

        eventHandler.onMessage(message);

        verify(dummyEventHandler, times(1)).handleEvent(any());
    }

    @Test
    public void GIVEN_eventOnDummyChannelWithExecutionContext_WHEN_handleEvent_THEN_methodWithExecutionContextIsUsed() throws JsonProcessingException {
        TestEvent testEvent = new TestEvent("1", "2");
        Message message = MessageBuilder.withBody(new ObjectMapper().writeValueAsBytes(testEvent)).build();
        message.getMessageProperties().setHeaders(new HashMap<>());
        message.getMessageProperties().getHeaders().put(Headers.ACCESS_TOKEN, "testJwt");
        message.getMessageProperties().getHeaders().put(Headers.USER_ID, "dummy-test-user");

        eventHandler.onMessage(message);

        verify(dummyEventHandler, times(0)).handleEvent(any(), eq(new ExecutionContext(UserId.valueOf("dummy-test-user"), "testJwt", UUID.randomUUID().toString())));
    }

    @Test
    public void GIVEN_eventOnDifferentChannel_WHEN_handleEvent_THEN_noHandleIsCalled() throws JsonProcessingException {
        TestEvent testEvent = new TestEvent("1", "2");
        Message message = MessageBuilder.withBody(new ObjectMapper().writeValueAsBytes(testEvent)).build();
        message.getMessageProperties().setHeaders(new HashMap<>());
        message.getMessageProperties().getHeaders().put(Headers.CHANNEL, "different");

        eventHandler.onMessage(message);

        verify(dummyEventHandler, times(0)).handleEvent(any());
    }


}
