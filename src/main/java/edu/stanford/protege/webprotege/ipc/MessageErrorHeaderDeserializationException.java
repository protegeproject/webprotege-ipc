package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;

public class MessageErrorHeaderDeserializationException extends MessageProcessingException {

    private final String errorHeader;

    public MessageErrorHeaderDeserializationException(String errorHeader, IOException cause) {
        super(getExceptionMessage(errorHeader, cause), cause);
        this.errorHeader = errorHeader;
    }

    @JsonProperty("errorHeader")
    public String getErrorHeader() {
        return errorHeader;
    }

    private static String getExceptionMessage(String errorHeader, IOException cause) {
        return "Error deserializing message error header. Error header: " + errorHeader + " Error: " + cause.getMessage();
    }
}
