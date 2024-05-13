package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.common.WebProtegeCommonConfiguration;
import edu.stanford.protege.webprotege.ipc.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(WebProtegeCommonConfiguration.class)
@EnableCaching
@ConfigurationPropertiesScan
public class WebProtegeIpcApplication {

	private static final Logger logger = LoggerFactory.getLogger(WebProtegeIpcApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(WebProtegeIpcApplication.class, args);
	}

	@Bean
	MessageChannelMapper messageChannelMapper(@Value("${spring.application.name}") String serviceName) {
		return new MessageChannelMapper(serviceName);
	}

	@Bean
	EventDispatcher eventDispatcher(ObjectMapper objectMapper, RabbitTemplate eventRabbitTemplate) {
		return new RabbitMQEventDispatcher(objectMapper,eventRabbitTemplate);
	}

	@Bean
	CommandExecutorImpl<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> executorForGetAuthorizationStatusRequest() {
		return new CommandExecutorImpl<>(GetAuthorizationStatusResponse.class);
	}


}
