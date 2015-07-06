package net.redborder.correlation.siddhi.exceptions;

public class AlreadyExistsException extends ExecutionPlanException {
    public AlreadyExistsException(String message) {
        super(message);
    }

    public AlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
