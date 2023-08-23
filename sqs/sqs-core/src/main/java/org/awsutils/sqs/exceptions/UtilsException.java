package org.awsutils.sqs.exceptions;

public class UtilsException extends ServiceException {
    public UtilsException(String errorType) {
        super(errorType);
    }

    public UtilsException(String errorType, String message) {
        super(errorType, message);
    }

    public UtilsException(String errorType, String message, Throwable cause) {
        super(errorType, message, cause);
    }

    public UtilsException(String errorType, Throwable cause) {
        super(errorType, cause);
    }
}
