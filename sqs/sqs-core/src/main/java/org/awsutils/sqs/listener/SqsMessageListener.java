package org.awsutils.sqs.listener;

import org.awsutils.sqs.client.SyncSqsMessageClient;
import org.awsutils.sqs.config.WorkerNodeCheckFunc;
import org.awsutils.sqs.handler.MessageHandlerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

public interface SqsMessageListener {
    void receive();

    static Builder builder(){
        return SqsMessageListenerImpl.builder();
    }

    interface Builder {
        Builder queueName(String queueName);

        Builder queueUrl(String queueUrl);

        Builder messageHandlerFactory(MessageHandlerFactory messageHandlerFactory);

        Builder executorService(ExecutorService executorService);

        Builder maximumNumberOfMessagesKey(String maximumNumberOfMessagesKey);

        Builder propertyReaderFunction(Function<String, Integer> propertyReaderFunction);

        Builder workerNodeCheck(WorkerNodeCheckFunc workerNodeCheck);

        Builder semaphore(Semaphore semaphore);

        Builder listenerName(String listenerName);

        Builder rateLimiterName(String rateLimiterName);

        Builder messageHandlerRateLimiter(String messageHandlerRateLimiter);

        Builder statusProperty(String enabled);

        Builder waitTimeInSeconds(Integer waitTimeInSeconds);

        Builder sqsSyncClient(SqsClient sqsSyncClient);

        Builder syncSqsMessageClient(SyncSqsMessageClient syncSqsMessageClient);

        SqsMessageListener build();
    }
}
