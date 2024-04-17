package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.common.UserId;
import edu.stanford.protege.webprotege.ipc.impl.CommandExecutorImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
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
public class CommandExecutor_CommandHandler_Test extends IntegrationTestsExtension {

    @Autowired
    CommandExecutor<TestRequest, TestResponse> executor;

    @Autowired
    ApplicationContext applicationContext;

    @BeforeEach
    void setUp() {

    }

    @Test
    void shouldAutowireCommandExecutor() {
        assertThat(executor).isNotNull();
    }

    @Test
    void shouldSendAndReceivedCommand() throws  ExecutionException, InterruptedException, TimeoutException {
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
            return new CommandExecutorImpl<>(TestResponse.class);
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
