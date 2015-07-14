package net.redborder.cep.siddhi.exceptions;

public class TransformException extends ExecutionPlanException {
    public TransformException(String message) {
        super(message);
    }

    public TransformException(String message, Throwable cause) {
        super(message, cause);
    }
}
