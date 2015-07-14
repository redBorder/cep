package net.redborder.cep.rest.exceptions;

public class RestInvalidException extends RestException {
    public RestInvalidException(String message) {
        super(message);
    }

    public RestInvalidException(String message, Throwable cause) {
        super(message, cause);
    }
}
