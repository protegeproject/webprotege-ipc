package edu.stanford.protege.webprotege.ipc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2023-05-23
 */
@SpringBootTest(properties = {"spring.mongodb.embedded.version=5.0.6"})
@AutoConfigureWebTestClient
@Testcontainers
public class IntegrationTestsExtension {

    private static Logger logger = LoggerFactory.getLogger(IntegrationTestsExtension.class);
    @Container
    static RabbitMQContainer rabbitContainer = new RabbitMQContainer("rabbitmq:3.7.25-management-alpine")
            .withStartupTimeout(Duration.ofSeconds(60)) // Increase startup timeout if needed
            .waitingFor(
                    Wait.forLogMessage(".*Server startup complete.*\\n", 1)
            );

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", rabbitContainer::getHost);
        registry.add("spring.rabbitmq.port", rabbitContainer::getAmqpPort);
    }
}
