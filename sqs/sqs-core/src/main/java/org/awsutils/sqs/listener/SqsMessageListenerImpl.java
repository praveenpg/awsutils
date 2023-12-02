package org.awsutils.sqs.listener;

import io.vavr.Tuple;
import io.vavr.Tuple3;
import lombok.extern.slf4j.Slf4j;
import org.awsutils.common.exceptions.UtilsException;
import org.awsutils.common.ratelimiter.RateLimiter;
import org.awsutils.common.ratelimiter.RateLimiterFactory;
import org.awsutils.common.util.ApplicationContextUtils;
import org.awsutils.common.util.Utils;
import org.awsutils.sqs.client.MessageConstants;
import org.awsutils.sqs.client.SyncSqsMessageClient;
import org.awsutils.sqs.config.WorkerNodeCheckFunc;
import org.awsutils.sqs.handler.MessageHandlerFactory;
import org.awsutils.sqs.handler.SqsMessageHandler;
import org.awsutils.sqs.message.SnsSubscriptionMessage;
import org.awsutils.sqs.message.SqsMessage;
import org.awsutils.sqs.message.TaskInput;
import org.springframework.core.env.Environment;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings({"unused", "ClassWithTooManyFields"})
@Slf4j
final class SqsMessageListenerImpl implements SqsMessageListener {
    @SuppressWarnings("FieldCanBeLocal")
    private final Environment environment;
    private String queueUrl;
    private final SqsClient sqsSyncClient;
    private final MessageHandlerFactory messageHandlerFactory;
    private final SyncSqsMessageClient syncSqsMessageClient;
    private final String rateLimiterName;
    private final ExecutorService executorService;
    private final String maximumNumberOfMessagesKey;
    private final Function<String, Integer> propertyReaderFunction;
    private final WorkerNodeCheckFunc workerNodeCheck;
    private final Semaphore semaphore;
    private final String listenerName;
    private final String messageHandlerRateLimiterName;
    private RateLimiter rateLimiter = new PassthroughRateLimiter();
    private RateLimiter messageHandlerRateLimiter = new PassthroughRateLimiter();
    private final boolean listenerEnabled;
    private final Integer waitTimeInSeconds;
    private static final Integer MAX_NUMBER_OF_SQS_MESSAGES = 10;
    private static final int MAXIMUM_NUMBER_OF_MESSAGES = 2000;
    private static final long SEMAPHORE_TIMEOUT_IN_SECONDS = 15L;
    private static final long CHANGE_VISIBILITY_PERIOD_IN_SECONDS = TimeUnit.MINUTES.toSeconds(15L);
    @SuppressWarnings("InstantiatingAThreadWithDefaultRunMethod")
    private static final Thread SHUTDOWN_HOOK = new Thread();


    private SqsMessageListenerImpl(final String queueUrl,
                                   final SqsClient sqsSyncClient,
                                   final MessageHandlerFactory messageHandlerFactory,
                                   final SyncSqsMessageClient syncSqsMessageClient,
                                   final ExecutorService executorService,
                                   final String rateLimiterName,
                                   final String maximumNumberOfMessagesKey,
                                   final Semaphore semaphore,
                                   final Function<String, Integer> propertyReaderFunction,
                                   final WorkerNodeCheckFunc workerNodeCheck,
                                   final String listenerName,
                                   final String messageHandlerRateLimiterName,
                                   final String statusProperty,
                                   final Integer waitTimeInSeconds) {


        final var timer = new Timer();
        this.sqsSyncClient = sqsSyncClient;
        this.syncSqsMessageClient = syncSqsMessageClient;
        this.rateLimiterName = rateLimiterName;
        this.messageHandlerRateLimiterName = messageHandlerRateLimiterName;
        this.waitTimeInSeconds = waitTimeInSeconds;
        this.messageHandlerFactory = messageHandlerFactory;
        this.propertyReaderFunction = propertyReaderFunction;
        this.listenerName = listenerName;
        this.executorService = executorService;
        this.maximumNumberOfMessagesKey = maximumNumberOfMessagesKey;
        this.workerNodeCheck = workerNodeCheck == null ? () -> true : workerNodeCheck;
        this.semaphore = semaphore == null ? new Semaphore(BigInteger.ONE.intValue()) : semaphore;
        this.environment = ApplicationContextUtils.getInstance().getBean(Environment.class);
        this.listenerEnabled = StringUtils.hasLength(statusProperty) ?
                environment.getProperty(statusProperty, Boolean.class, true) : true;
        this.queueUrl = queueUrl;

        if (!StringUtils.hasLength(queueUrl)) {
            throw new IllegalStateException("QueueUrl is required");
        }
        if (log.isInfoEnabled()) {
            log.info("Creating SqsMessageListener: {}, Queue: {}", listenerName, queueUrl);
        }

        timer.schedule(new DefaultTimerTask(this::setRateLimiters), 10000);
    }

    private String getQueueUrl(final SyncSqsMessageClient sqsMessageClient, final String queueName) {
        if (!StringUtils.hasLength(this.queueUrl)) {
            synchronized (this) {
                if (!StringUtils.hasLength(this.queueUrl)) {
                    this.queueUrl = sqsMessageClient.getQueueUrl(queueName);
                }
            }
        }

        return this.queueUrl;
    }

    public void receive() {
        try {
            if (listenerEnabled && workerNodeCheck.check()) {
                log.debug("Receiving messages after starter in listener [{}]",
                        listenerName);
                processUsingLock();
            } else {
                log.debug("Not receiving messages since worker node check returned false");
            }
        } catch (final InterruptedException e) {
            Utils.handleInterruptedException(e, () -> {});
        }
    }

    @SuppressWarnings("ConstantConditions")
    private void setRateLimiters() {
        try {
            if (StringUtils.hasLength(rateLimiterName)) {
                this.rateLimiter = RateLimiterFactory.getInstance().getRateLimiter(rateLimiterName);
            }

            if (StringUtils.hasLength(messageHandlerRateLimiterName)) {
                this.messageHandlerRateLimiter = RateLimiterFactory.getInstance()
                        .getRateLimiter(messageHandlerRateLimiterName);
            }
        } catch (final Exception ex) {
            log.error("Exception: {}", ex, ex);

            throw ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex);
        }
    }

    private void processUsingLock() throws InterruptedException {
        Utils.executeUsingSemaphore(semaphore, SEMAPHORE_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS, () -> {
            var messageCounter = 0;
            var proceed = true;
            log.debug("Checking for messages from SQS in listener [{}]: {}", listenerName, queueUrl);

            while (proceed) {
                final var messages = receiveMessages();

                messageCounter += messages.size();

                proceed = processSqsMessages(messages, messageCounter);

                log.debug("Proceed with receiving messages [{}]: {}", listenerName, proceed);
            }

            if (log.isDebugEnabled()) {
                log.debug("Total number of messages received: {}", messageCounter);
                log.debug("Rate limiter used: {}", (rateLimiter != null ? rateLimiter.getRateLimiterName() : null));
            }
        });
    }

    private boolean processSqsMessages(final List<Message> messages, final int messageCounter) {
        final boolean proceed;

        log.debug("In processSqsMessages: " + messages);

        if (!CollectionUtils.isEmpty(messages)) {
            log.debug("Message list is not empty..");
            messages.forEach(this::processSqsMessage);

            proceed = (!StringUtils.hasLength(maximumNumberOfMessagesKey) ? MAXIMUM_NUMBER_OF_MESSAGES :
                    propertyReaderFunction.apply(maximumNumberOfMessagesKey)) > messageCounter;
        } else {
            log.debug("List of messages is empty in listener [{}]: {}", listenerName, 0);

            proceed = false;
        }

        return proceed;
    }

    @SuppressWarnings("FunctionalExpressionCanBeFolded")
    private void processSqsMessage(final Message message) {
        final var startTime = System.currentTimeMillis();
        final var changeVisibilityResp = syncSqsMessageClient
                .changeVisibility(queueUrl, message.receiptHandle(),
                        (int) CHANGE_VISIBILITY_PERIOD_IN_SECONDS);
        final Runnable action0 = () -> {
            log.debug("Processing message: " + message.messageId());

            executorService.submit(() -> processMessage(message, startTime));
        };

        if (rateLimiter == null) {
            action0.run();
        } else {
            rateLimiter.execute(action0::run);
        }
    }

    private void processMessage(final Message message, final long startTime) {
        try {
            log.debug("Processing message in listener[{}]: {}", listenerName, message);
            final var timeTakenToStartProcessing = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()
                    - startTime);

            if (timeTakenToStartProcessing < CHANGE_VISIBILITY_PERIOD_IN_SECONDS) {
                final Tuple3<SqsMessage<?>, Map<String, String>, TaskInput<?>> sqsMessage;
                final var body = message.body();
                final var receiptHandle = message.receiptHandle();
                final var receiveCount = message.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT);
                final SqsMessageHandler<?> messageHandler;
                final var messageAttributes = message.messageAttributes().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, a -> a.getValue().stringValue()));
                final var sqsMessageWrapperPresent = messageAttributes.get(MessageConstants.SQS_MESSAGE_WRAPPER_PRESENT);

                if ((StringUtils.hasLength(sqsMessageWrapperPresent) && "true".equalsIgnoreCase(sqsMessageWrapperPresent))
                        || message.body().contains("\"messageType")) {
                    sqsMessage = constructSqsMessage(body, receiptHandle);
                    messageHandler = messageHandlerFactory.getMessageHandler(sqsMessage._1(),
                            receiptHandle,
                            queueUrl,
                            StringUtils.hasLength(receiveCount) ? Integer.parseInt(receiveCount) : 0,
                            CollectionUtils.isEmpty(messageAttributes) ? sqsMessage._2() : messageAttributes,
                            messageHandlerRateLimiter
                    );

                    log.debug("Handling message by {}", messageHandler);
                } else if (StringUtils.hasLength(messageAttributes.get(MessageConstants.MESSAGE_TYPE))) {
                    final var messageType = messageAttributes.get(MessageConstants.MESSAGE_TYPE);
                    final var transactionId = messageAttributes.get(MessageConstants.TRANSACTION_ID);

                    messageHandler = messageHandlerFactory.getMessageHandler(body,
                            messageType,
                            transactionId,
                            receiptHandle,
                            queueUrl,
                            StringUtils.hasLength(receiveCount) ? Integer.parseInt(receiveCount) : 0,
                            messageAttributes,
                            messageHandlerRateLimiter);
                } else {
                    throw new UtilsException("INVALID_MESSAGE", "The message body should be of SqsMessage type or " +
                            "should contain `messageType` attribute");
                }

                messageHandler.handle();
            }
        } catch (final UtilsException e) {
            handleUtilsException(message, e);
        } catch (final Exception e) {
            log.error("Exception in listener[{}]: {}", listenerName, e.getMessage(), e);
        }
    }

    private void handleUtilsException(final Message message, final UtilsException e) {
        if ("NO_HANDLER_FOR_MESSAGE_TYPE".equalsIgnoreCase(e.getErrorType()) || "INVALID_JSON"
                .equalsIgnoreCase(e.getErrorType())) {
            log.error("Exception in listener[{}]: {}", listenerName, e.getMessage(), e);
            syncSqsMessageClient.deleteMessage(queueUrl, message.receiptHandle());
        } else {
            log.error("Exception in listener[{}]: {}", listenerName, e.getMessage(), e);
        }
    }

    private Tuple3<SqsMessage<?>, Map<String, String>, TaskInput<?>> constructSqsMessage(final String body,
                                                                                         final String receiptHandle) {

        final SqsMessage<?> sqsMessage;
        final Map<String, String> attributes;

        if (!body.contains("\"Type\" : \"Notification\"")) {
            sqsMessage = Utils.constructFromJson(SqsMessage.class, body, cause -> new UtilsException("INVALID_JSON", cause));
            return Tuple.of(sqsMessage, null, null);
        } else {
            return processSnsNotification(body, receiptHandle);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Tuple3<SqsMessage<?>, Map<String, String>, TaskInput<?>> processSnsNotification(final String body,
                                                                                            final String receiptHandle) {

        final SqsMessage<?> sqsMessage;
        final var snsSubscriptionMessage = Utils.constructFromJson(SnsSubscriptionMessage.class, body);
        final var snsMessage = snsSubscriptionMessage.getMessage();
        final var messageAttributes = snsSubscriptionMessage.getMessageAttributes();
        final TaskInput taskInput;
        final Map<String, String> messAttr;

        if (snsMessage.contains("\"Input\"")) {
            taskInput = Utils.constructFromJson(TaskInput.class, snsMessage);

            if (taskInput.getInput() == null) {
                syncSqsMessageClient.deleteMessage(queueUrl, receiptHandle);

                throw new UtilsException("EMPTY_MESSAGE_BODY", "Empty sqs message body");
            }

            sqsMessage = taskInput.getInput();
        } else {
            taskInput = null;
            sqsMessage = Utils.constructFromJson(SqsMessage.class, snsMessage, cause
                    -> new UtilsException("INVALID_JSON", cause));
        }
        messAttr = CollectionUtils.isEmpty(messageAttributes) ? null :
                messageAttributes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        b -> b.getValue().getValue()));

        return Tuple.of(sqsMessage, messAttr, taskInput);
    }


    private List<Message> receiveMessages() {
        final var request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.ALL)
                .messageAttributeNames("All")
                .maxNumberOfMessages(MAX_NUMBER_OF_SQS_MESSAGES)
                .waitTimeSeconds(waitTimeInSeconds)
                .build();

        return sqsSyncClient.receiveMessage(request).messages();
    }

    static SqsMessageListener.Builder builder() {
        return new SqsMessageListenerBuilder();
    }

    private static class SqsMessageListenerBuilder implements SqsMessageListener.Builder {
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
        public Builder queueUrl(final String queueUrl) {
            this.queueUrl = queueUrl;
            return this;
        }

        @Override
        public Builder messageHandlerFactory(final MessageHandlerFactory messageHandlerFactory) {
            this.messageHandlerFactory = messageHandlerFactory;
            return this;
        }

        @Override
        public Builder executorService(final ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        @Override
        public Builder maximumNumberOfMessagesKey(final String maximumNumberOfMessagesKey) {
            this.maximumNumberOfMessagesKey = maximumNumberOfMessagesKey;
            return this;
        }

        @Override
        public Builder propertyReaderFunction(final Function<String, Integer> propertyReaderFunction) {
            this.propertyReaderFunction = propertyReaderFunction;
            return this;
        }

        @Override
        public Builder workerNodeCheck(final WorkerNodeCheckFunc workerNodeCheck) {
            this.workerNodeCheck = workerNodeCheck;

            return this;
        }

        @Override
        public Builder semaphore(final Semaphore semaphore) {
            this.semaphore = semaphore;

            return this;
        }

        @Override
        public Builder listenerName(final String listenerName) {
            this.listenerName = listenerName;

            return this;
        }

        @Override
        public Builder rateLimiterName(final String rateLimiterName) {
            this.rateLimiterName = rateLimiterName;

            return this;
        }

        @Override
        public Builder messageHandlerRateLimiter(final String messageHandlerRateLimiter) {
            this.messageHandlerRateLimiter = messageHandlerRateLimiter;

            return this;
        }

        @Override
        public Builder statusProperty(final String enabled) {
            this.statusProperty = enabled;

            return this;
        }

        @Override
        public Builder waitTimeInSeconds(final Integer waitTimeInSeconds) {
            this.waitTimeInSeconds = waitTimeInSeconds;

            return this;
        }

        @Override
        public Builder sqsSyncClient(SqsClient sqsSyncClient) {
            this.sqsSyncClient = sqsSyncClient;

            return this;
        }

        @Override
        public Builder syncSqsMessageClient(SyncSqsMessageClient syncSqsMessageClient) {
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
}
