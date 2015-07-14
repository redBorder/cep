package net.redborder.cep.rest.exceptions;

public class RestServerErrorException extends RestException {
    public RestServerErrorException(String message) {
        super(message);
    }

    public RestServerErrorException(String message, Throwable cause) {
        super(message, cause);
    }
}
