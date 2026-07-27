package edu.stanford.protege.webprotege.ipc.impl;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the events queue bean construction — verifies the
 * {@code webprotege.rabbitmq.event-queue-per-instance} flag selects the right kind of
 * queue without needing a broker.
 */
class RabbitMQEventsConfigurationTest {

    @Test
    void defaultModeProducesSharedDurableQueue() {
        var config = new RabbitMQEventsConfiguration();
        config.eventsQueue = "EVENTS_QUEUE";
        config.eventQueuePerInstance = false;

        Queue queue = config.eventsQueue();

        assertThat(queue.getName()).isEqualTo("EVENTS_QUEUE");
        assertThat(queue.isDurable()).isTrue();
        assertThat(queue.isExclusive()).isFalse();
        assertThat(queue.isAutoDelete()).isFalse();
    }

    @Test
    void perInstanceModeProducesUniqueTransientExclusiveAutoDeleteQueue() {
        var config = new RabbitMQEventsConfiguration();
        config.eventsQueue = "EVENTS_QUEUE";
        config.eventQueuePerInstance = true;

        Queue queue = config.eventsQueue();

        // Uniquely named, using the configured queue name as a prefix.
        assertThat(queue.getName()).startsWith("EVENTS_QUEUE-");
        assertThat(queue.getName()).isNotEqualTo("EVENTS_QUEUE");
        assertThat(queue.isDurable()).isFalse();
        assertThat(queue.isExclusive()).isTrue();
        assertThat(queue.isAutoDelete()).isTrue();
    }

    @Test
    void perInstanceModeProducesADifferentNamePerInstance() {
        var config = new RabbitMQEventsConfiguration();
        config.eventsQueue = "EVENTS_QUEUE";
        config.eventQueuePerInstance = true;

        // Each instance (each bean creation) gets its own distinct queue name, so two
        // instances never share a queue and therefore never compete for events.
        assertThat(config.eventsQueue().getName()).isNotEqualTo(config.eventsQueue().getName());
    }
}
