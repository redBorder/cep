package net.redborder.correlation.siddhi.exceptions;

public class InvalidExecutionPlanException extends ExecutionPlanException {
    public InvalidExecutionPlanException(String message) {
        super(message);
    }

    public InvalidExecutionPlanException(String message, Throwable cause) {
        super(message, cause);
    }
}
