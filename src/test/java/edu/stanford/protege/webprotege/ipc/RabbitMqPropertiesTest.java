package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.ipc.impl.RabbitMqProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2024-05-11
 */
@SpringBootTest(properties = {
        "webprotege.rabbitmq.timeout=12345",
        "webprotege.rabbitmq.commands-subscribe=false",
        "webprotege.rabbitmq.requestqueue=abc",
        "webprotege.rabbitmq.responsequeue=def"
})
public class RabbitMqPropertiesTest {

    @Autowired
    private RabbitMqProperties properties;

    @Test
    void shouldReadTimeout() {
        assertThat(properties.getTimeout()).isEqualTo(12345);
    }

    @Test
    void shouldReadCommandsSubscribe() {
        assertThat(properties.getCommandsSubscribe()).isEqualTo(false);
    }

    @Test
    void shouldReadRequestQueue() {
        assertThat(properties.getRequestqueue()).isEqualTo("abc");
    }

    @Test
    void shouldReadResponseQueue() {
        assertThat(properties.getResponsequeue()).isEqualTo("def");
    }
}
