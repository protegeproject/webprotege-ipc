package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.common.ProjectId;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-01-08
 */
public class KafkaListenerEventRecordHandlerWrapper {

    public static final String METHOD_NAME = "handleEventRecord";

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerCommandHandlerWrapper.class);

    private final EventRecordHandler eventRecordHandler;
    
    public KafkaListenerEventRecordHandlerWrapper(EventRecordHandler handler) {
        this.eventRecordHandler = Objects.requireNonNull(handler);
    }

    public void handleEventRecord(final ConsumerRecord<String, String> record) {
        var eventType = extractEventType(record);
        var projectId = extractProjectId(record);
        var payload = record.value();
        var timestamp = record.timestamp();

        var eventRecord = new EventRecord("", timestamp, eventType, payload, projectId);
        try {
            eventRecordHandler.handleEventRecord(eventRecord);
        }
        catch (Exception e) {
            logger.info("An unhandled exception occurred while asking a listener to handle an event {} {}",
                        e.getClass().getSimpleName(),
                        e.getMessage());
        }
    }

    private String extractEventType(ConsumerRecord<String, String> record) {
        return new String(record.headers().lastHeader(Headers.EVENT_TYPE).value(), StandardCharsets.UTF_8);
    }

    private ProjectId extractProjectId(ConsumerRecord<String, String> record) {
        var projectIdHeader = record.headers().lastHeader(Headers.PROJECT_ID);
        if(projectIdHeader == null) {
            return null;
        }
        var projectIdBytes = projectIdHeader.value();
        return getProjectId(projectIdBytes);
    }

    private static ProjectId getProjectId(@Nullable byte [] projectIdBytes) {
        if(projectIdBytes == null) {
            return null;
        }
        return ProjectId.valueOf(new String(projectIdBytes, StandardCharsets.UTF_8));
    }
}
