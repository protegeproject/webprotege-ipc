package edu.stanford.protege.webprotege.ipc.impl;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2024-05-11
 */
@Configuration
@ConfigurationProperties(prefix = "webprotege.rabbitmq")
public class RabbitMqProperties {

    private boolean commandsSubscribe;

    private String requestqueue;

    private String responsequeue;

    private long timeout;

    public boolean getCommandsSubscribe() {
        return commandsSubscribe;
    }

    public void setCommandsSubscribe(boolean commandsSubscribe) {
        this.commandsSubscribe = commandsSubscribe;
    }

    public String getRequestqueue() {
        return requestqueue;
    }

    public void setRequestqueue(String requestqueue) {
        this.requestqueue = requestqueue;
    }

    public String getResponsequeue() {
        return responsequeue;
    }

    public void setResponsequeue(String responsequeue) {
        this.responsequeue = responsequeue;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}
