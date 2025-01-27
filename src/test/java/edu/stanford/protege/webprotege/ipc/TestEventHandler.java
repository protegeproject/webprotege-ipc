package edu.stanford.protege.webprotege.ipc;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

@WebProtegeHandler
public class TestEventHandler implements EventHandler<TestEvent>{

    private final static Logger logger = LoggerFactory.getLogger(TestEventHandler.class);

    private boolean handledEvents = false;

    private TestEvent handledEvent;

    @Nonnull
    @Override
    public String getChannelName() {
        return TestEvent.CHANNEL;
    }

    @Nonnull
    @Override
    public String getHandlerName() {
        return "TheTestEventHandler";
    }

    @Override
    public Class<TestEvent> getEventClass() {
        return TestEvent.class;
    }

    public boolean isHandledEvents() {
        return handledEvents;
    }

    @Override
    public void handleEvent(TestEvent event) {
        logger.info("Handling event " + event);
        handledEvents = true;
        handledEvent = event;
        EventHandler_TestCase.countDownLatch.countDown();
    }

    @Override
    public void handleEvent(TestEvent event, ExecutionContext executionContext) {

    }

    public TestEvent getHandledEvent() {
        return handledEvent;
    }
}
