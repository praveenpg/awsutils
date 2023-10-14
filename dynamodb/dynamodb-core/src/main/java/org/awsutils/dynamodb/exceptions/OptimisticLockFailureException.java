package org.awsutils.dynamodb.exceptions;


@SuppressWarnings({"unused", "RedundantSuppression"})
public class OptimisticLockFailureException extends DbException {
    public OptimisticLockFailureException() {
        super("OPTIMISTIC_LOCK_ERROR");
    }

    public OptimisticLockFailureException(final Throwable cause) {
        super("OPTIMISTIC_LOCK_ERROR", cause);
    }

    public OptimisticLockFailureException(final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super("OPTIMISTIC_LOCK_ERROR", cause, enableSuppression, writableStackTrace);
    }
}
