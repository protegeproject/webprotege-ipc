package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-07-30
 */
public class MessageChannelMapper {

    private static final String REPLY_CHANNEL_SUFFIX = "-Replies";

    private final String serviceName;

    public MessageChannelMapper(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getChannelName(Request<? extends Response> request) {
        return request.getChannel();
    }

    public String getReplyChannelName(Request<? extends Response> request) {
        return serviceName + "-" + getChannelName(request) + REPLY_CHANNEL_SUFFIX;
    }
}
