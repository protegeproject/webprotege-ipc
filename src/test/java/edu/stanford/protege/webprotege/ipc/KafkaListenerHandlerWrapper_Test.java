package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class KafkaListenerHandlerWrapper_Test {

//    private KafkaListenerHandlerWrapper wrapper;
//
//    @Mock
//    private KafkaTemplate<String, String> kafkaTemplate;
//
//    @Mock
//    private ObjectMapper objectMapper;
//
//    @Mock
//    private CommandHandler<?, ?> commandHandler;
//
//    @Mock
//    private ProducerRecord<String, String> producerRecord;
//
//    private Headers headers;
//
//    @BeforeEach
//    void setUp() {
//        wrapper = new KafkaListenerHandlerWrapper<>(kafkaTemplate, objectMapper, commandHandler);
//        headers = new RecordHeaders();
//    }
//
//    @Test
//    void shouldHandleForbiddenException() {
//        when(commandHandler.handleRequest(any(), any())).thenReturn(Mono.error())
//        var value = "Value";
//        var theTopic = "TheTopic";
//        var consumerRecord = createConsumerRecord(value, theTopic);
//        wrapper.handleMessage(consumerRecord);
//    }
//
//    @Test
//    void shouldHandleForbiddenException() {
//        when(commandHandler.handleRequest(any(), any())).thenReturn(Mono.error())
//        var value = "Value";
//        var theTopic = "TheTopic";
//        var consumerRecord = createConsumerRecord(value, theTopic);
//        wrapper.handleMessage(consumerRecord);
//    }
//
//    private ConsumerRecord<String, String> createConsumerRecord(String value, String theTopic) {
//        return new ConsumerRecord<>(theTopic, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "Key", value, headers);
//    }
}