package net.redborder.cep.siddhi.exceptions;

public class InvalidExecutionPlanException extends ExecutionPlanException {
    public InvalidExecutionPlanException(String message) {
        super(message);
    }

    public InvalidExecutionPlanException(String message, Throwable cause) {
        super(message, cause);
    }
}
