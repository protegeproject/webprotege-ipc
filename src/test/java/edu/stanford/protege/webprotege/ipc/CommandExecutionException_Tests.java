package edu.stanford.protege.webprotege.ipc;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.AutoConfigureJsonTesters;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-09-01
 */
@SpringBootTest
@AutoConfigureJsonTesters
@ContextConfiguration(classes = WebProtegeIpcApplication.class)
@TestPropertySource(properties = "webprotege.rabbitmq.commands-subscribe=false")
public class CommandExecutionException_Tests extends IntegrationTestsExtension {

    @Autowired
    private JacksonTester<CommandExecutionException> tester;

    private final HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;

    @Test
    void shouldSerializeException() throws IOException {
        var exception = new CommandExecutionException(status);
        var json = tester.write(exception);
        assertThat(json).extractingJsonPathNumberValue("statusCode").isEqualTo(500);
        assertThat(json).doesNotHaveJsonPath("cause");
        assertThat(json).doesNotHaveJsonPath("message");
        assertThat(json).doesNotHaveJsonPath("stackTrace");
        assertThat(json).doesNotHaveJsonPath("status");
    }

    @Test
    void shouldSerializeExceptionWithMessage() throws IOException {
        var exception = new CommandExecutionException(status, NullPointerException.class.getName(), "NPE");
        var json = tester.write(exception);
        assertThat(json).extractingJsonPathNumberValue("statusCode").isEqualTo(500);
        assertThat(json).doesNotHaveJsonPath("cause");
        assertThat(json).doesNotHaveJsonPath("message");
        assertThat(json).doesNotHaveJsonPath("stackTrace");
        assertThat(json).doesNotHaveJsonPath("status");
        assertThat(json).extractingJsonPathStringValue("causeMessage").isEqualTo("NPE");
        assertThat(json).extractingJsonPathStringValue("causeClassName").isEqualTo(NullPointerException.class.getName());
    }

    @Test
    void shouldSerializeExceptionFromFactoryMethod() throws IOException {
        var exception = CommandExecutionException.of(new NullPointerException());
        var json = tester.write(exception);
        assertThat(json).extractingJsonPathNumberValue("statusCode").isEqualTo(500);
        assertThat(json).doesNotHaveJsonPath("cause");
        assertThat(json).doesNotHaveJsonPath("message");
        assertThat(json).doesNotHaveJsonPath("stackTrace");
        assertThat(json).doesNotHaveJsonPath("status");
        assertThat(json).extractingJsonPathStringValue("causeMessage").isEqualTo("");
        assertThat(json).extractingJsonPathStringValue("causeClassName").isEqualTo(NullPointerException.class.getName());
    }

    @Test
    void shouldDeserializeException() throws IOException {
        var parsed = tester.parse("""
                     {
                        "statusCode" : 500
                     }
                     """);
        var parsedObject = parsed.getObject();
        assertThat(parsedObject.getStatusCode()).isEqualTo(500);
        assertThat(parsedObject.getStatus()).isEqualTo(status);
        assertThat(parsedObject.getCauseMessage()).isEmpty();
        assertThat(parsedObject.getCauseClassName()).isEmpty();

    }
}
