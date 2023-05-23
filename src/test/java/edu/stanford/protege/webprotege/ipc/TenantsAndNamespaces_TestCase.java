package edu.stanford.protege.webprotege.ipc;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.io.UncheckedIOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-08
 */
@SpringBootTest
@ExtendWith(PulsarTestExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class TenantsAndNamespaces_TestCase {

    @Autowired
    private PulsarAdmin pulsarAdmin;

    @Value("${webprotege.pulsar.tenant}")
    private String expectedTenant;

    @BeforeEach
    void setUp() {
        System.out.println(System.identityHashCode(pulsarAdmin));
        System.out.println(expectedTenant);
    }

    @Test
    void shouldCreateTenant() throws PulsarAdminException {
        assertThat(pulsarAdmin.tenants().getTenants()).contains(expectedTenant);
    }

    @Test
    void shouldCreateCommandRequestNamespace() throws PulsarAdminException {
        assertThat(pulsarAdmin.namespaces().getNamespaces(expectedTenant)).contains(expectedTenant + "/command-requests");
    }

    @Test
    void shouldCreateCommandRepliesNamespace() throws PulsarAdminException {
        assertThat(pulsarAdmin.namespaces().getNamespaces(expectedTenant)).contains(expectedTenant + "/command-responses");
    }

    @Test
    void shouldSetExpirationTimeOnCommandRepliesNamespace() throws PulsarAdminException {
        assertThat(pulsarAdmin.namespaces().getSubscriptionExpirationTime(expectedTenant + "/command-responses")).isNotEqualTo(0);
    }

    @Test
    void shouldCreateEventsNamespace() throws PulsarAdminException {
        assertThat(pulsarAdmin.namespaces().getNamespaces(expectedTenant)).contains(expectedTenant + "/events");
    }

    @AfterEach
    void tearDown() throws PulsarAdminException {
    }
}
