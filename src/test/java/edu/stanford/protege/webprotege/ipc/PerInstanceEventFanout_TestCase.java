package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.ProjectId;
import edu.stanford.protege.webprotege.ipc.impl.RabbitMQEventsConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@code webprotege.rabbitmq.event-queue-per-instance} against a real
 * (Testcontainers) RabbitMQ broker. They simulate two service instances by wiring two
 * listener containers to the fanout exchange, then publish a single event through the real
 * {@link EventDispatcher} and assert the delivery fan-out:
 *
 * <ul>
 *   <li>per-instance mode (each instance has its own anonymous, exclusive, auto-delete queue):
 *       <em>both</em> instances receive the one event;</li>
 *   <li>default mode (both instances share one durable queue): <em>exactly one</em> instance
 *       receives it — the historical competing-consumer semantics that must not change.</li>
 * </ul>
 *
 * This deliberately does not enable {@code event-subscribe}, so the production listener/binding
 * beans stay inactive and the test owns the whole events topology.
 */
@SpringBootTest(classes = WebProtegeIpcApplication.class)
class PerInstanceEventFanout_TestCase extends IntegrationTestsExtension {

    @Autowired
    private EventDispatcher eventDispatcher;

    @Autowired
    private ConnectionFactory connectionFactory;

    private RabbitAdmin rabbitAdmin;

    private final List<SimpleMessageListenerContainer> containers = new ArrayList<>();
    private final List<String> declaredQueues = new ArrayList<>();

    private void setUpAdmin() {
        rabbitAdmin = new RabbitAdmin(connectionFactory);
        rabbitAdmin.declareExchange(fanoutExchange());
    }

    @AfterEach
    void tearDown() {
        containers.forEach(SimpleMessageListenerContainer::stop);
        containers.clear();
        if (rabbitAdmin != null) {
            declaredQueues.forEach(rabbitAdmin::deleteQueue);
        }
        declaredQueues.clear();
    }

    @Test
    void perInstanceModeDeliversTheEventToEveryInstance() throws InterruptedException {
        setUpAdmin();
        var bothReceived = new CountDownLatch(2);

        // Two "instances", each with its own anonymous exclusive/auto-delete queue (exactly what
        // RabbitMQEventsConfiguration builds when event-queue-per-instance=true) bound to the fanout.
        registerInstance(perInstanceQueue(), bothReceived::countDown, new AtomicInteger());
        registerInstance(perInstanceQueue(), bothReceived::countDown, new AtomicInteger());

        publishOneEvent();

        assertThat(bothReceived.await(30, TimeUnit.SECONDS))
                .as("both per-instance queues should each receive the single published event")
                .isTrue();
    }

    @Test
    void defaultModeDeliversTheEventToExactlyOneInstance() throws InterruptedException {
        setUpAdmin();
        var totalDeliveries = new AtomicInteger();
        var anyReceived = new CountDownLatch(1);

        // Two "instances" sharing ONE durable queue: competing consumers on the same queue.
        var sharedQueue = sharedDurableQueue();
        registerInstance(sharedQueue, anyReceived::countDown, totalDeliveries);
        registerInstance(sharedQueue, anyReceived::countDown, totalDeliveries);

        publishOneEvent();

        assertThat(anyReceived.await(30, TimeUnit.SECONDS))
                .as("one of the two competing consumers should receive the event")
                .isTrue();
        // Give any (erroneous) second delivery time to arrive before asserting exactly-once.
        Thread.sleep(2000);
        assertThat(totalDeliveries.get())
                .as("a single shared queue must deliver the event to exactly one consumer")
                .isEqualTo(1);
    }

    private FanoutExchange fanoutExchange() {
        return new FanoutExchange(RabbitMQEventsConfiguration.EVENT_EXCHANGE, true, false);
    }

    private Queue perInstanceQueue() {
        return new AnonymousQueue(new Base64UrlNamingStrategy("test-events-"));
    }

    private Queue sharedDurableQueue() {
        return new Queue("test-shared-events-queue-" + UUID.randomUUID(), true);
    }

    /**
     * Declares {@code queue}, binds it to the fanout exchange and starts a listener container on
     * it whose message listener counts the delivery and runs {@code onDelivery}. Waits until the
     * container's consumer is actually attached so a subsequent publish cannot race consumer setup.
     */
    private void registerInstance(Queue queue, Runnable onDelivery, AtomicInteger counter) {
        rabbitAdmin.declareQueue(queue);
        declaredQueues.add(queue.getActualName());
        rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(fanoutExchange()));

        var container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueues(queue);
        container.setAcknowledgeMode(AcknowledgeMode.AUTO);
        container.setMessageListener((MessageListener) message -> {
            counter.incrementAndGet();
            onDelivery.run();
        });
        container.afterPropertiesSet();
        container.start();
        containers.add(container);
        waitForConsumer(container);
    }

    private void waitForConsumer(SimpleMessageListenerContainer container) {
        var deadline = Instant.now().plus(Duration.ofSeconds(20));
        while (container.getActiveConsumerCount() < 1 && Instant.now().isBefore(deadline)) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        assertThat(container.getActiveConsumerCount())
                .as("listener container should have an active consumer before publishing")
                .isGreaterThanOrEqualTo(1);
    }

    private void publishOneEvent() {
        eventDispatcher.dispatchEvent(new TestEvent(UUID.randomUUID().toString(), ProjectId.generate().id()));
    }
}
