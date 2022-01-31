package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.common.WebProtegeCommonConfiguration;
import edu.stanford.protege.webprotege.ipc.kafka.KafkaCommandExecutor;
import edu.stanford.protege.webprotege.ipc.kafka.KafkaEventDispatcher;
import edu.stanford.protege.webprotege.ipc.kafka.ReplyingKafkaTemplateFactory;
import edu.stanford.protege.webprotege.ipc.kafka.ReplyingKafkaTemplateFactoryImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@SpringBootApplication
@Import(WebProtegeCommonConfiguration.class)
public class WebProtegeIpcApplication {

	public static void main(String[] args) {
		SpringApplication.run(WebProtegeIpcApplication.class, args);
	}

	@Bean
	MessageChannelMapper messageChannelMapper(@Value("${spring.application.name}") String serviceName) {
		return new MessageChannelMapper(serviceName);
	}

	@Bean
	ReplyErrorChecker replyErrorChecker(ObjectMapper objectMapper) {
		return new ReplyErrorChecker(objectMapper);
	}

	@Bean
    ReplyingKafkaTemplateFactory replyingKafkaTemplateFactory(ConcurrentKafkaListenerContainerFactory<String, String> containerFactory,
                                                              ProducerFactory<String, String> producerFactory,
                                                              ReplyErrorChecker replyErrorChecker) {
		return new ReplyingKafkaTemplateFactoryImpl(containerFactory, producerFactory, replyErrorChecker);
	}

	@Bean
	EventDispatcher eventDispatcher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
		return new KafkaEventDispatcher(kafkaTemplate, objectMapper);
	}

	@Bean
	KafkaCommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> executorForGetAuthorizationStatusRequest() {
		return new KafkaCommandExecutor<>(GetAuthorizationStatusResponse.class);
	}
}
