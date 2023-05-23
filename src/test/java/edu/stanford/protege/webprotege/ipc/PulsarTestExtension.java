package edu.stanford.protege.webprotege.ipc;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2023-05-23
 */
public class PulsarTestExtension implements BeforeAllCallback, AfterAllCallback {

    private PulsarContainer pulsarContainer;


    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        var imageName = DockerImageName.parse("apachepulsar/pulsar");
        pulsarContainer = new PulsarContainer(imageName)
                .withCommand("standalone")
                .withExposedPorts(6650, 8080);
        pulsarContainer.start();

        System.setProperty("webprotege.pulsar.serviceHttpUrl", "http://localhost:" + pulsarContainer.getMappedPort(8080));
        System.setProperty("webprotege.pulsar.serviceUrl", "pulsar://localhost:" + pulsarContainer.getMappedPort(6650));
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        pulsarContainer.stop();
    }
}
