package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.function.Function;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-23
 */
public class ReplyErrorChecker implements Function<ConsumerRecord<?, ?>, Exception> {

    private final ObjectMapper objectMapper;

    private static final Logger logger = LoggerFactory.getLogger(ReplyErrorChecker.class);

    public ReplyErrorChecker(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Exception apply(ConsumerRecord<?, ?> consumerRecord) {
        var errorHeader = consumerRecord.headers()
                .lastHeader(Headers.ERROR);
        if(errorHeader == null) {
            return null;
        }
        var errorValue = errorHeader.value();
        if(errorValue == null) {
            return null;
        }
        try {
            return objectMapper.readValue(errorValue, CommandExecutionException.class);
        } catch (IOException e) {
            logger.error("Error while deserializing CommandExecutionException", e);
            return new CommandExecutionException(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
