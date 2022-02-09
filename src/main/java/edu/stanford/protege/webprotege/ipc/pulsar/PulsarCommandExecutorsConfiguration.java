package edu.stanford.protege.webprotege.ipc.pulsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

import static edu.stanford.protege.webprotege.ipc.pulsar.PulsarNamespaces.COMMAND_REPLIES;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-08
 */
@Configuration
public class PulsarCommandExecutorsConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(PulsarCommandExecutorsConfiguration.class);

    private final PulsarAdmin pulsarAdmin;

    private final String tenant;

    private final int commandSubscriptionExpiraryTime;

    public PulsarCommandExecutorsConfiguration(PulsarAdmin pulsarAdmin,
                                               @Value("${webprotege.pulsar.tenant}") String tenant,
                                               @Value("${webprotege.pulsar.command-replies.subScriptionExpiraryTimeMillis}") int commandSubscriptionExpiraryTime) {
        this.pulsarAdmin = pulsarAdmin;
        this.tenant = tenant;
        this.commandSubscriptionExpiraryTime = commandSubscriptionExpiraryTime;
    }

    @PostConstruct
    public void setSubscriptionExpirationsForCommands() {
        logger.info("Setting subscription expiration time for command reply subscriptions to {} minutes", commandSubscriptionExpiraryTime);
        pulsarAdmin.namespaces()
                .setSubscriptionExpirationTimeAsync(tenant + "/" + COMMAND_REPLIES, commandSubscriptionExpiraryTime);
    }

}
