package com.treasure_data.model;

public class APIException extends Exception {

    private static final long serialVersionUID = 1L;

    public APIException(Throwable cause) {
        super(cause);
    }

    public APIException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public APIException(String reason) {
        super(reason);
    }
}
