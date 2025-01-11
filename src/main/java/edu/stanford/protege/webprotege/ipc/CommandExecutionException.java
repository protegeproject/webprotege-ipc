package edu.stanford.protege.webprotege.ipc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.checkerframework.checker.units.qual.C;
import org.springframework.http.HttpStatus;

import java.util.Objects;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-23
 */
@JsonIncludeProperties({"statusCode", "causeClassName", "causeMessage"})
public class CommandExecutionException extends RuntimeException {

    private final int statusCode;

    private final String causeClassName;

    private final String causeMessage;

    public CommandExecutionException(HttpStatus status) {
        this(status, "", "");
    }

    public CommandExecutionException(HttpStatus status, String causeClassName, String causeMessage) {
        this(status.value(), causeClassName, causeMessage);
    }

    @JsonCreator
    public CommandExecutionException(@JsonProperty("statusCode") int statusCode,
                                     @JsonProperty("causeClassName") String causeClassName,
                                     @JsonProperty("causeMessage") String causeMessage) {
        this.statusCode = statusCode;
        this.causeClassName = Objects.requireNonNullElse(causeClassName, "");
        this.causeMessage = Objects.requireNonNullElse(causeMessage, "");
    }

    /**
     * Creates a {@link CommandExecutionException} that describes the throwable.  If the specific throwable is
     * a {@link CommandExecutionException} then the specified throwable is simply returned.
     * @param throwable The throwable for which to create a {@link CommandExecutionException}
     */
    public static CommandExecutionException of(Throwable throwable) {
        if(throwable instanceof CommandExecutionException commandExecutionException) {
            return commandExecutionException;
        }
        return new CommandExecutionException(HttpStatus.INTERNAL_SERVER_ERROR,
                throwable.getClass().getName(),
                throwable.getMessage());
    }

    public static CommandExecutionException of(Throwable throwable, HttpStatus httpStatus) {
        return new CommandExecutionException(httpStatus, throwable.getClass().getName(), throwable.getMessage());
    }

    public static CommandExecutionException of(Throwable throwable, HttpStatus httpStatus, String causeMessage) {
        return new CommandExecutionException(httpStatus, throwable.getClass().getName(), causeMessage);
    }

    public static CommandExecutionException of(HttpStatus httpStatus, String errorMessage) {
        return new CommandExecutionException(httpStatus, "", errorMessage);
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
        return "Error " + statusCode + " (" + getStatus().getReasonPhrase() + ") [" + causeClassName + "] " + causeMessage;
    }

    @JsonProperty("causeClassName")
    public String getCauseClassName() {
        return causeClassName;
    }

    @JsonProperty("causeMessage")
    public String getCauseMessage() {
        return causeMessage;
    }




}
