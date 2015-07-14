package net.redborder.cep.rest.exceptions;

public class RestNotFoundException extends RestException {
    public RestNotFoundException(String message) {
        super(message);
    }

    public RestNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
