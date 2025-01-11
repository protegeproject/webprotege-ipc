package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Request;

public class MessageBodySerializationException extends MessageProcessingException {

    public MessageBodySerializationException(Request<?> request, Throwable cause) {
        super(getExceptionMessage(request, cause), cause);
    }

    private static String getExceptionMessage(Request<?> request, Throwable cause) {
        return "Error serializing request to JSON.  Request: " + request + " Cause: " + cause;
    }
}
