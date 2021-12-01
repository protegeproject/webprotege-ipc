package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-04
 */
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
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
        public String getChannel() {
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
