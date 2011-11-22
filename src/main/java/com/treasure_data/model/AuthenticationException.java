package com.treasure_data.model;

public class AuthenticationException extends APIException {

    private static final long serialVersionUID = 1L;

    public AuthenticationException(Throwable cause) {
        super(cause);
    }

    public AuthenticationException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public AuthenticationException(String reason) {
        super(reason);
    }
}
