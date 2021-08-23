package edu.stanford.protege.webprotege.ipc;

import org.springframework.http.HttpStatus;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-23
 */
public class CommandExecutionException extends RuntimeException {

    private final HttpStatus status;

    public CommandExecutionException(HttpStatus status) {
        this.status = status;
    }

    public HttpStatus getStatus() {
        return status;
    }
}
