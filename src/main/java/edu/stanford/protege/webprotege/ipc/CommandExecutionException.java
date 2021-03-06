package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.http.HttpStatus;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-23
 */
@JsonIncludeProperties("statusCode")
public class CommandExecutionException extends RuntimeException {

    private final int statusCode;

    public CommandExecutionException(HttpStatus status) {
        this.statusCode = status.value();
    }

    @JsonCreator
    public CommandExecutionException(@JsonProperty("statusCode") int statusCode) {
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    @JsonIgnore
    public HttpStatus getStatus() {
        return HttpStatus.resolve(statusCode);
    }

    @Override
    public String getMessage() {
        return "HTTP Error " + statusCode + " (" + getStatus().getReasonPhrase() + ")";
    }
}
