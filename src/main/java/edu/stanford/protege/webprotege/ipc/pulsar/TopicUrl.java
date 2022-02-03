package edu.stanford.protege.webprotege.ipc.pulsar;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2022-02-02
 */
public class TopicUrl {

    public static String getCommandTopicUrl(String channel) {
        return "persistent://webprotege/commands/" + channel;
    }

    public static String getEventTopicUrl(String channel) {
        return "persistent://webprotege/events/" + channel;
    }
}
