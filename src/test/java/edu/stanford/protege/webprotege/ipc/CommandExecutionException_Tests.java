package edu.stanford.protege.webprotege.ipc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.AutoConfigureJsonTesters;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-09-01
 */
@SpringBootTest
@ExtendWith(PulsarTestExtension.class)
@AutoConfigureJsonTesters
@ContextConfiguration(classes = WebProtegeIpcApplication.class)
public class CommandExecutionException_Tests {

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
    void shouldDeserializeException() throws IOException {
        var parsed = tester.parse("""
                     {
                        "statusCode" : 500
                     }
                     """);
        var parsedObject = parsed.getObject();
        assertThat(parsedObject.getStatusCode()).isEqualTo(500);
        assertThat(parsedObject.getStatus()).isEqualTo(status);

    }
}
