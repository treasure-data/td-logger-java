package com.treasure_data.model;

public class AlreadyExistsException extends APIException {

    private static final long serialVersionUID = 1L;

    public AlreadyExistsException(Throwable cause) {
        super(cause);
    }

    public AlreadyExistsException(String reason) {
        super(reason);
    }

    public AlreadyExistsException(String reason,Throwable cause) {
        super(reason, cause);
    }
}