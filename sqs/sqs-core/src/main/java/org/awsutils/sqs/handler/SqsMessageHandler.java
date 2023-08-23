package org.awsutils.sqs.handler;

import java.util.concurrent.CompletableFuture;

public interface SqsMessageHandler<T> {
    void handle();

    CompletableFuture<?> execute(T message);

    void handleException(final T message, final Throwable exception);

    T getMessage();

    void handleSuccess();
}
