package edu.stanford.protege.webprotege.ipc.event;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-04
 */
public class EventDispatchException extends RuntimeException {

    public EventDispatchException(Throwable cause) {
        super("Error serializing event to JSON", cause);
    }
}
