package org.awsutils.common.exceptions;

public class ServiceException extends RuntimeException {
    private final String errorType;

    public ServiceException(String errorType) {
        this.errorType = errorType;
    }

    public ServiceException(String errorType, String message) {
        super(message);
        this.errorType = errorType;
    }

    public ServiceException(String errorType, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
    }

    public ServiceException(String errorType, Throwable cause) {
        super(cause);
        this.errorType = errorType;
    }

    public String getErrorType() {
        return errorType;
    }
}
