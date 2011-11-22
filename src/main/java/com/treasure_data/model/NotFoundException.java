package com.treasure_data.model;

public class NotFoundException extends APIException {

    private static final long serialVersionUID = 1L;

    public NotFoundException(Throwable cause) {
        super(cause);
    }

    public NotFoundException(String reason) {
        super(reason);
    }

    public NotFoundException(String reason, Throwable cause) {
        super(reason, cause);
    }
}
