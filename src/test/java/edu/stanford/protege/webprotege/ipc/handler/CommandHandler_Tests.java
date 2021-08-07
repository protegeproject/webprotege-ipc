package edu.stanford.protege.webprotege.ipc.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.WebProtegeCommonConfiguration;
import edu.stanford.protege.webprotege.ipc.cmd.ReplyingKafkaTemplateFactory;
import edu.stanford.protege.webprotege.ipc.cmd.Request;
import edu.stanford.protege.webprotege.ipc.cmd.Response;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-06
 */
@SpringBootTest
public class CommandHandler_Tests {

    @Autowired
    private CountDownLatch countDownLatch;

    @Autowired
    private TestHandler testHandler;

    @Autowired
    private ReplyingKafkaTemplateFactory replyingKafkaTemplateFactory;

    @Autowired
    ObjectMapper objectMapper;


    @Test
    void shouldInitializeContext() {
        assertThat(testHandler).isNotNull();
    }

    @Test
    void shouldReceiveMessageOnHandler() throws InterruptedException, ExecutionException, JsonProcessingException, TimeoutException {
        var template = replyingKafkaTemplateFactory.create("TheTestTopic-Replies");
        template.start();
        var payload = objectMapper.writeValueAsString(new TestRequest("Hello World"));
        var msg = MessageBuilder.withPayload(payload)
                                         .setHeader(KafkaHeaders.TOPIC, "TheTestTopic")
                                         .setReplyChannelName("TheTestTopic-Replies")
                                         .build();
        var result = template.sendAndReceive(msg, Duration.ofMillis(4000000));
        var replyPayload = result.get(400, TimeUnit.SECONDS).getPayload();
        var response = objectMapper.readValue((String) replyPayload, TestRespose.class);
        assertThat(response.content()).isEqualTo("HELLO WORLD");
        countDownLatch.await();
    }

    @TestConfiguration
    static class Config {

        @Bean
        CountDownLatch countDownLatch() {
            return new CountDownLatch(1);
        }

        @Bean
        TestHandler testHandler() {
            return new TestHandler(countDownLatch());
        }

    }

    public static class TestHandler implements CommandHandler<TestRequest, TestRespose> {

        private final CountDownLatch countDownLatch;

        public TestHandler(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Nonnull
        @Override
        public String getChannelName() {
            return "TheTestTopic";
        }

        @Override
        public Class<TestRequest> getRequestClass() {
            return TestRequest.class;
        }

        @Override
        public Mono<TestRespose> handleRequest(TestRequest request) {
            System.out.println("Handling record: " + request);
            countDownLatch.countDown();
            return Mono.just(new TestRespose(request.content().toUpperCase()));
        }

    }

    private record TestRequest(String content) implements Request<TestRespose> {

        @Override
        public String getChannel() {
            return "TheTestTopic";
        }
    }

    private static record TestRespose(String content) implements Response {

    }

}
