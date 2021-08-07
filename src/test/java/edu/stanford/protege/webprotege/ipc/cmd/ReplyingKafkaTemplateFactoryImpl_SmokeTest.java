package edu.stanford.protege.webprotege.ipc.cmd;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-03
 */
@SpringBootTest
public class ReplyingKafkaTemplateFactoryImpl_SmokeTest {

    @Autowired
    private ReplyingKafkaTemplateFactory factory;

    @Test
    void shouldCreateFactory() {
        assertThat(factory).isNotNull();
    }
}
