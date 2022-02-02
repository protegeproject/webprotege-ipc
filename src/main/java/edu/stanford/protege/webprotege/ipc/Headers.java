package edu.stanford.protege.webprotege.ipc;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-19
 */
public class Headers {

    private static final String PREFIX = "webprotege_";

    public static final String REPLY_CHANNEL = PREFIX + "replyChannel";

    public static final String CORRELATION_ID = PREFIX + "correlationId";

    public static final String USER_ID = PREFIX + "userId";

    public static final String ERROR = PREFIX + "error";

    public static final String EVENT_TYPE = PREFIX + "eventType";

    public static final String PROJECT_ID = PREFIX + "projectId";
}
