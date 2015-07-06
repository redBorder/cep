package net.redborder.correlation.siddhi.exceptions;

public class ExecutionPlanException extends Exception {
    public ExecutionPlanException(String message) {
        super(message);
    }

    public ExecutionPlanException(String message, Throwable cause) {
        super(message, cause);
    }
}
