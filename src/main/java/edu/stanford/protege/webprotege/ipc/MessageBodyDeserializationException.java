package edu.stanford.protege.webprotege.ipc;

import org.springframework.util.StringUtils;

import java.io.IOException;

public class MessageBodyDeserializationException extends MessageProcessingException {

    private final byte [] messageBody;

    public MessageBodyDeserializationException(byte [] messageBody, IOException cause) {
        super(getExceptionMessage(messageBody, cause), cause);
        this.messageBody = messageBody;
    }

    private static String getExceptionMessage(byte [] messageBody, IOException cause) {
        var truncatedMessageBody = StringUtils.truncate(new String(messageBody), 100);
        return "Error deserializing reply message body. Cause: " + cause.getMessage() + " Body: " + truncatedMessageBody;
    }

    public byte [] getMessageBody() {
        return messageBody;
    }
}
