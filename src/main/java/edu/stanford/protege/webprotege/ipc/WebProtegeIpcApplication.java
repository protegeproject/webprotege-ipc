package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusRequest;
import edu.stanford.protege.webprotege.authorization.GetAuthorizationStatusResponse;
import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import edu.stanford.protege.webprotege.common.WebProtegeCommonConfiguration;
import edu.stanford.protege.webprotege.ipc.pulsar.*;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@Import(WebProtegeCommonConfiguration.class)
@EnableCaching
public class WebProtegeIpcApplication {

	private static final Logger logger = LoggerFactory.getLogger(WebProtegeIpcApplication.class);

	@Value("${webprotege.pulsar.tenant}")
	private String tenant;

	@Value("${webprotege.pulsar.serviceHttpUrl}")
	private String serviceHttpUrl;

	@Value("${webprotege.pulsar.serviceUrl}")
	private String pulsarServiceUrl;

	public static void main(String[] args) {
		SpringApplication.run(WebProtegeIpcApplication.class, args);
	}

	@Bean
	MessageChannelMapper messageChannelMapper(@Value("${spring.application.name}") String serviceName) {
		return new MessageChannelMapper(serviceName);
	}

	@Bean
	EventDispatcher eventDispatcher(@Value("${spring.application.name}") String applicationName,
									PulsarProducersManager pulsarProducersManager, ObjectMapper objectMapper) {
		return new PulsarEventDispatcher(applicationName, pulsarProducersManager, objectMapper, tenant);
	}

	@Bean
	PulsarCommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> executorForGetAuthorizationStatusRequest() {
		return new PulsarCommandExecutor<>(GetAuthorizationStatusResponse.class);
	}

	@Bean
	PulsarAdmin pulsarAdmin() {
		try {
			var admin = new PulsarAdminBuilderImpl()
					.serviceHttpUrl(serviceHttpUrl)
					.build();
			createTenantIfNecessary(admin);
			createNamespaceIfNecessary(admin, PulsarNamespaces.COMMAND_REQUESTS);
			createNamespaceIfNecessary(admin, PulsarNamespaces.COMMAND_REPLIES);
			createNamespaceIfNecessary(admin, PulsarNamespaces.EVENTS);
			return admin;
		} catch (PulsarClientException | PulsarAdminException e) {
			throw new RuntimeException(e);
		}
	}

	private void createTenantIfNecessary(PulsarAdmin admin) throws PulsarAdminException {
		if(!admin.tenants().getTenants().contains(tenant)) {
			logger.info("Creating Pulsar tenant: {}", tenant);
			admin.tenants().createTenant(tenant,
										 new TenantInfoImpl(Set.of("admin"), Set.of("standalone")));
		}
	}

	private void createNamespaceIfNecessary(PulsarAdmin admin, String namespace) throws PulsarAdminException {
		var namespaceName = tenant + "/" + namespace;
		if(!admin.namespaces().getNamespaces(tenant).contains(namespaceName)) {
			logger.info("Creating Pulsar namespace: {}", namespaceName);
			admin.namespaces().createNamespace(namespaceName);
		}
	}

	@Bean
	PulsarClient pulsarClient() throws PulsarClientException {
		return PulsarClient.builder()
						   .serviceUrl(pulsarServiceUrl).build();
	}

	@Bean
	PulsarProducersManager pulsarProducersManager(PulsarClient pulsarClient,
												  @Value("${spring.application.name}") String applicationName) {
		return new PulsarProducersManager(pulsarClient, applicationName);
	}

	@Bean
	PulsarCommandHandlerWrapperFactory pulsarCommandHandlerWrapperFactory(@Value("${spring.application.name}") String applicationName,
																		  ObjectMapper objectMapper,
																		  PulsarProducersManager producersManager,
																		  CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor,
																		  PulsarClient pulsarClient) {

		return new PulsarCommandHandlerWrapperFactory() {
			@Override
			public <Q extends Request<R>, R extends Response> PulsarCommandHandlerWrapper<Q, R> create(CommandHandler<Q, R> handler) {
				return pulsarCommandHandlerWrapper(handler,
														applicationName,
														pulsarClient,
														objectMapper,
														producersManager,
														authorizationStatusExecutor);
			}
		};
	}

	@Bean
	@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	public  <Q extends Request<R>, R extends Response> PulsarCommandHandlerWrapper<Q, R> pulsarCommandHandlerWrapper(
			CommandHandler<Q, R> handler,
			String applicationName,
			PulsarClient pulsarClient,
			ObjectMapper objectMapper,
			PulsarProducersManager producersManager,
			CommandExecutor<GetAuthorizationStatusRequest, GetAuthorizationStatusResponse> authorizationStatusExecutor) {
		return new PulsarCommandHandlerWrapper<>(applicationName,
												 tenant,
												 pulsarClient,
												 handler,
												 objectMapper,
												 producersManager,
												 authorizationStatusExecutor);
	}

	@Bean
	Caffeine<Object, Object> pulsarProducerCaffeineConfig() {
		return Caffeine.newBuilder()
					   .expireAfterAccess(10, TimeUnit.MINUTES);
	}

	@Bean
	public CacheManager cacheManager(Caffeine<Object, Object> caffeine) {
		var caffeineCacheManager = new CaffeineCacheManager();
		caffeineCacheManager.setCaffeine(caffeine);
		return caffeineCacheManager;
	}
}
