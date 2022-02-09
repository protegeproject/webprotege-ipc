package edu.stanford.protege.webprotege.ipc;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-08
 */
public class PulsarTestUtils {

    public static void deleteTestTenant(PulsarAdmin pulsarAdmin, String tenant) throws PulsarAdminException {
        pulsarAdmin.namespaces().getNamespaces(tenant)
                   .forEach(ns -> {
                       try {
                           pulsarAdmin.namespaces().getTopics(ns)
                                   .forEach(topic -> {
                                       try {
                                           pulsarAdmin.topics().delete(topic, true);
                                       } catch (PulsarAdminException e) {
                                           throw new RuntimeException(e);
                                       }
                                   });
                           pulsarAdmin.namespaces().deleteNamespace(ns);
                       } catch (PulsarAdminException e) {
                           throw new RuntimeException(e);
                       }
                   });
        pulsarAdmin.tenants().deleteTenant(tenant);
    }
}
