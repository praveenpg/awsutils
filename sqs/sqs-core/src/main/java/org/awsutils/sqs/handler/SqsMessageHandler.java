package org.awsutils.sqs.handler;

public interface SqsMessageHandler<T> {
    void handle();

    <X> X execute(T message);

    void handleException(final T message, final Throwable exception);

    T getMessage();

    void handleSuccess();
}
