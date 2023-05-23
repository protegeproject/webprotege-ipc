package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.common.UserId;
import edu.stanford.protege.webprotege.ipc.pulsar.PulsarCommandExecutor;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-03
 */
@SpringBootTest
@ExtendWith(PulsarTestExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class CommandExecutor_CommandHandler_Test {

    @Autowired
    CommandExecutor<TestRequest, TestResponse> executor;

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    PulsarClient pulsarClient;

    @Autowired
    private PulsarAdmin pulsarAdmin;

    @Value("${webprotege.pulsar.tenant}")
    private String tenant;


    @BeforeEach
    void setUp() {

    }

    @AfterEach
    void tearDown() throws PulsarAdminException {
    }


    @Test
    void shouldAutowireCommandExecutor() {
        assertThat(executor).isNotNull();
    }

    @Test
    void shouldSendAndReceivedCommand() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        var id = UUID.randomUUID().toString();
        var response = executor.execute(new TestRequest(id), new ExecutionContext(new UserId("JohnSmith"),
                                                                                "access-token-value"));
        var res = response.get(5000, TimeUnit.SECONDS);
        assertThat(res).isNotNull();
        assertThat(res.getId()).isEqualTo(id);
    }

    @TestConfiguration
    public static class CommandExecutorConfig {

        /**
         * The bean for the {@link CommandExecutor}.  This is part of the smoke test, checking
         * that everything can be instantiated
         */
        @Bean
        CommandExecutor<TestRequest, TestResponse> commandExecutor() {
            return new PulsarCommandExecutor<>(TestResponse.class);
        }

        @Bean
        CommandHandler<TestRequest, TestResponse> commandHandler() {
            return new TestCommandHandler();
        }

    }




    @JsonTypeName("TestRequest")
    private static class TestRequest implements Request<TestResponse> {

        private static final String CHANNEL = "webprotege-tests.test-request";

        private final String id;

        @JsonCreator
        public TestRequest(@JsonProperty("id") String id) {
            this.id = id;
        }

        @Override
        public String getChannel() {
            return CHANNEL;
        }

        public String getId() {
            return id;
        }
    }

    @JsonTypeName("TestResponse")
    private static class TestResponse implements Response {

        private final String id;

        @JsonCreator
        public TestResponse(@JsonProperty("id") String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }

    @WebProtegeHandler
    private static class TestCommandHandler implements CommandHandler<TestRequest, TestResponse> {

        @Nonnull
        @Override
        public String getChannelName() {
            return TestRequest.CHANNEL;
        }

        @Override
        public Class<TestRequest> getRequestClass() {
            return TestRequest.class;
        }

        @Override
        public Mono<TestResponse> handleRequest(TestRequest request, ExecutionContext executionContext) {
            return Mono.just(new TestResponse(request.getId()));
        }
    }

}
