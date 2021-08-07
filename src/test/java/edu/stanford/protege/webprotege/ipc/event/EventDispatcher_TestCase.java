package edu.stanford.protege.webprotege.ipc.event;

import edu.stanford.protege.webprotege.common.WebProtegeCommonConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-04
 */
@SpringBootTest
public class EventDispatcher_TestCase {

    public static final String THE_EVENT_ID = "TheEventId";

    @Autowired
    private EventDispatcher eventDispatcher;

    private static CountDownLatch countDownLatch;

    @BeforeEach
    void setUp() {
        countDownLatch = new CountDownLatch(1);
    }

    @Test
    void shouldInstantiateEventDispatcher() {
        assertThat(eventDispatcher).isNotNull();
    }

    @Test
    void shouldDispatchEvent() throws InterruptedException {
        var event = new TestEvent(THE_EVENT_ID);
        eventDispatcher.dispatchEvent(event);
        countDownLatch.await();
    }


    private static record TestEvent(String id) implements Event {

        @Override
        public String getChannelName() {
            return "TestEventChannel";
        }
    }

    @TestConfiguration
    static class Config {

        @KafkaListener(topics = "TestEventChannel")
        void handleEvent(String event) {
            countDownLatch.countDown();
        }
    }
}
