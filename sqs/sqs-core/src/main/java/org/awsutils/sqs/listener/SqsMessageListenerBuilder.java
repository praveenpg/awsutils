package org.awsutils.sqs.listener;

import org.awsutils.sqs.client.SyncSqsMessageClient;
import org.awsutils.sqs.config.WorkerNodeCheckFunc;
import org.awsutils.sqs.handler.MessageHandlerFactory;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.lang.reflect.Proxy;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

final class SqsMessageListenerBuilder implements SqsMessageListener.Builder {
    private MessageHandlerFactory messageHandlerFactory;
    private ExecutorService executorService;
    private String maximumNumberOfMessagesKey;
    private Function<String, Integer> propertyReaderFunction;
    private WorkerNodeCheckFunc workerNodeCheck;
    private Semaphore semaphore;
    private String listenerName;
    private String rateLimiterName;
    private String messageHandlerRateLimiter;
    private String statusProperty;
    private Integer waitTimeInSeconds;
    private String queueUrl;

    private SqsClient sqsSyncClient;

    private SyncSqsMessageClient syncSqsMessageClient;

    @Override
    public SqsMessageListener.Builder queueUrl(final String queueUrl) {
        this.queueUrl = queueUrl;
        return this;
    }

    @Override
    public SqsMessageListener.Builder messageHandlerFactory(final MessageHandlerFactory messageHandlerFactory) {
        this.messageHandlerFactory = messageHandlerFactory;
        return this;
    }

    @Override
    public SqsMessageListener.Builder executorService(final ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    @Override
    public SqsMessageListener.Builder maximumNumberOfMessagesKey(final String maximumNumberOfMessagesKey) {
        this.maximumNumberOfMessagesKey = maximumNumberOfMessagesKey;
        return this;
    }

    @Override
    public SqsMessageListener.Builder propertyReaderFunction(final Function<String, Integer> propertyReaderFunction) {
        this.propertyReaderFunction = propertyReaderFunction;
        return this;
    }

    @Override
    public SqsMessageListener.Builder workerNodeCheck(final WorkerNodeCheckFunc workerNodeCheck) {
        this.workerNodeCheck = workerNodeCheck;

        return this;
    }

    @Override
    public SqsMessageListener.Builder semaphore(final Semaphore semaphore) {
        this.semaphore = semaphore;

        return this;
    }

    @Override
    public SqsMessageListener.Builder listenerName(final String listenerName) {
        this.listenerName = listenerName;

        return this;
    }

    @Override
    public SqsMessageListener.Builder rateLimiterName(final String rateLimiterName) {
        this.rateLimiterName = rateLimiterName;

        return this;
    }

    @Override
    public SqsMessageListener.Builder messageHandlerRateLimiter(final String messageHandlerRateLimiter) {
        this.messageHandlerRateLimiter = messageHandlerRateLimiter;

        return this;
    }

    @Override
    public SqsMessageListener.Builder statusProperty(final String enabled) {
        this.statusProperty = enabled;

        return this;
    }

    @Override
    public SqsMessageListener.Builder waitTimeInSeconds(final Integer waitTimeInSeconds) {
        this.waitTimeInSeconds = waitTimeInSeconds;

        return this;
    }

    @Override
    public SqsMessageListener.Builder sqsSyncClient(SqsClient sqsSyncClient) {
        this.sqsSyncClient = sqsSyncClient;

        return this;
    }

    @Override
    public SqsMessageListener.Builder syncSqsMessageClient(SyncSqsMessageClient syncSqsMessageClient) {
        this.syncSqsMessageClient = syncSqsMessageClient;

        return this;
    }

    @Override
    public SqsMessageListener build() {
        final SqsMessageListenerImpl sqsMessageListener = new SqsMessageListenerImpl(
                queueUrl, sqsSyncClient,
                messageHandlerFactory,
                syncSqsMessageClient, executorService,
                rateLimiterName,
                maximumNumberOfMessagesKey,
                semaphore,
                propertyReaderFunction,
                workerNodeCheck,
                !StringUtils.hasLength(listenerName) ? UUID.randomUUID().toString() : listenerName.trim(),
                messageHandlerRateLimiter,
                statusProperty,
                waitTimeInSeconds
        );

        return (SqsMessageListener) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[]{SqsMessageListener.class},
                (proxy, method, args) -> method.invoke(sqsMessageListener, args));
    }
}
