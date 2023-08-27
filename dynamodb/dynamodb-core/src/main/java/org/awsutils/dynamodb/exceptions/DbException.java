package org.awsutils.dynamodb.exceptions;

@SuppressWarnings({"unused", "RedundantSuppression"})
public class DbException extends RuntimeException {
    public DbException() {
    }

    public DbException(final String message) {
        super(message);
    }

    public DbException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public DbException(final Throwable cause) {
        super(cause);
    }

    public DbException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
