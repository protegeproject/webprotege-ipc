package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.common.UserId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;

import java.io.IOException;
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
public class CommandExecutor_Test {

    @Autowired
    CommandExecutor<TestRequest, TestResponse> executor;

    @Test
    void shouldAutowireCommandExecutor() {
        assertThat(executor).isNotNull();
    }

    @Test
    void shouldSendAndReceivedCommand() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        var response = executor.execute(new TestRequest(), new ExecutionContext(new UserId("JohnSmith")));
        var res = response.get(5000, TimeUnit.SECONDS);
        assertThat(res).isNotNull();
    }

    @TestConfiguration
    public static class CommandExecutorConfig {

        /**
         * The bean for the {@link CommandExecutor}.  This is part of the smoke test, checking
         * that everything can be instantiated
         */
        @Bean
        CommandExecutor<TestRequest, TestResponse> commandExecutor() {
            return new CommandExecutor<>(TestResponse.class);
        }

        /**
         * A consumer that can respond the the {@link TestRequest} message
         */
        @KafkaListener(topics = "TestRequest")
        @SendTo
        String listen(String request) {
            // Send back empty object as JSON
            return "{}";
        }
    }




    private static class TestRequest implements Request<TestResponse> {

        @Override
        public String getChannel() {
            return "TestRequest";
        }
    }

    private static class TestResponse implements Response {

    }

}
