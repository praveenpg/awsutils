package org.awsutils.sqs.listener;

import io.vavr.Tuple;
import io.vavr.Tuple3;
import org.awsutils.sqs.client.MessageConstants;
import org.awsutils.sqs.client.SqsMessageClient;
import org.awsutils.sqs.config.WorkerNodeCheckFunc;
import org.awsutils.common.exceptions.UtilsException;
import org.awsutils.sqs.handler.MessageHandlerFactory;
import org.awsutils.sqs.handler.SqsMessageHandler;
import org.awsutils.sqs.message.MessageAttribute;
import org.awsutils.sqs.message.SnsSubscriptionMessage;
import org.awsutils.sqs.message.SqsMessage;
import org.awsutils.sqs.message.TaskInput;
import org.awsutils.common.ratelimiter.RateLimiter;
import org.awsutils.common.ratelimiter.RateLimiterFactory;
import org.awsutils.common.util.ApplicationContextUtils;
import org.awsutils.common.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SuppressWarnings({"unused", "ClassWithTooManyFields"})
@Slf4j
final class SqsMessageListenerImpl implements SqsMessageListener {
    private final String queueName;
    @SuppressWarnings("FieldCanBeLocal")
    private final Environment environment;
    private String queueUrl;
    private final SqsAsyncClient sqsAsyncClient;
    private final MessageHandlerFactory messageHandlerFactory;
    private final SqsMessageClient sqsMessageClient;
    private final String rateLimiterName;
    private final ExecutorService executorService;
    private final String maximumNumberOfMessagesKey;
    private final Function<String, Integer> propertyReaderFunction;
    private final WorkerNodeCheckFunc workerNodeCheck;
    private final Semaphore semaphore;
    private final String listenerName;
    private final String messageHandlerRateLimiterName;
    private RateLimiter rateLimiter;
    private RateLimiter messageHandlerRateLimiter;
    private final Function<String, String> queueUrlFunc;
    private final boolean listenerEnabled;
    private final Integer waitTimeInSeconds;

    private static final Integer MAX_NUMBER_OF_SQS_MESSAGES = 10;
    private static final int MAXIMUM_NUMBER_OF_MESSAGES = 2000;
    private static final long SEMAPHORE_TIMEOUT_IN_SECONDS = 15L;
    private static final long CHANGE_VISIBILITY_PERIOD_IN_SECONDS = TimeUnit.MINUTES.toSeconds(15L);
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsMessageListenerImpl.class);
    @SuppressWarnings("InstantiatingAThreadWithDefaultRunMethod")
    private static final Thread SHUTDOWN_HOOK = new Thread();


    private SqsMessageListenerImpl(final SqsAsyncClient sqsAsyncClient,
                                   final String queueName,
                                   final String queueUrl,
                                   final SqsMessageClient sqsMessageClient,
                                   final MessageHandlerFactory messageHandlerFactory,
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

        this.rateLimiterName = rateLimiterName;
        this.messageHandlerRateLimiterName = messageHandlerRateLimiterName;
        this.waitTimeInSeconds = waitTimeInSeconds;
        this.sqsAsyncClient = sqsAsyncClient;
        this.messageHandlerFactory = messageHandlerFactory;
        this.propertyReaderFunction = propertyReaderFunction;
        this.listenerName = listenerName;
        this.sqsMessageClient = sqsMessageClient;
        this.executorService = executorService;
        this.maximumNumberOfMessagesKey = maximumNumberOfMessagesKey;
        this.workerNodeCheck = workerNodeCheck == null ? () -> true : workerNodeCheck;
        this.semaphore = semaphore == null ? new Semaphore(BigInteger.ONE.intValue()) : semaphore;
        this.environment = ApplicationContextUtils.getInstance().getBean(Environment.class);
        this.listenerEnabled = StringUtils.hasLength(statusProperty) ?
                environment.getProperty(statusProperty, Boolean.class, true) : true;
        this.queueName = queueName;
        this.queueUrl = queueUrl;
        this.queueUrlFunc = StringUtils.hasLength(queueUrl) ? qName -> this.queueUrl : qName
                -> getQueueUrl(sqsMessageClient, qName);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Creating SqsMessageListener: {}, Queue: {}", listenerName, queueName);
        }
    }

    private String getQueueUrl(final SqsMessageClient sqsMessageClient, final String queueName) {
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
        if (listenerEnabled && workerNodeCheck.check()) {
            setRateLimiters();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(MessageFormat.format("Receiving messages after starter in listener [{0}]",
                        listenerName));
            }

            try {
                processUsingLock();
            } catch (final InterruptedException e) {
                Utils.handleInterruptedException(e, () -> {});
            }
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Not receiving messages since worker node check returned false");
            }
        }
    }

    @SuppressWarnings("ConstantConditions")
    private void setRateLimiters() {
        try {
            if (rateLimiter == null && StringUtils.hasLength(rateLimiterName)) {
                this.rateLimiter = RateLimiterFactory.getInstance().getRateLimiter(rateLimiterName);
            }

            if (messageHandlerRateLimiter == null && StringUtils.hasLength(messageHandlerRateLimiterName)) {
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
            int messageCounter = 0;
            boolean proceed = true;

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(MessageFormat.format("Checking for messages from SQS in listener [{0}]: {1}",
                        listenerName, queueUrlFunc.apply(queueName)));
            }

            while (proceed) {
                final List<Message> messages = receiveMessages();

                messageCounter += messages.size();

                proceed = processSqsMessages(messages, messageCounter);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(MessageFormat.format("Proceed with receiving messages [{0}]: {1}", listenerName,
                            proceed));
                }
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(MessageFormat.format("Total number of messages received: {0}", messageCounter));
                LOGGER.debug(MessageFormat.format("Rate limiter used: {0}", (rateLimiter != null ?
                        rateLimiter.getRateLimiterName() : null)));
            }
        });
    }

    private boolean processSqsMessages(final List<Message> messages, final int messageCounter) {
        final boolean proceed;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("In processSqsMessages: " + messages);
        }


        if (!CollectionUtils.isEmpty(messages)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Message list is not empty..");
            }

            messages.forEach(this::processSqsMessage);

            proceed = (!StringUtils.hasLength(maximumNumberOfMessagesKey) ? MAXIMUM_NUMBER_OF_MESSAGES :
                    propertyReaderFunction.apply(maximumNumberOfMessagesKey)) > messageCounter;
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(MessageFormat.format("List of messages is empty in listener [{0}]: {1}",
                        listenerName, messages != null ? messages.size() : 0));
            }

            proceed = false;
        }

        return proceed;
    }

    @SuppressWarnings("FunctionalExpressionCanBeFolded")
    private void processSqsMessage(final Message message) {
        final long startTime = System.currentTimeMillis();
        final ChangeMessageVisibilityResponse changeVisibilityResp = getResultFromFuture(sqsMessageClient
                .changeVisibility(queueUrlFunc.apply(queueName), message.receiptHandle(),
                        (int) CHANGE_VISIBILITY_PERIOD_IN_SECONDS));
        final Runnable action0 = () -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Processing message: " + message.messageId());
            }

            executorService.submit(() -> processMessage(message, startTime));
        };

        if(rateLimiter == null) {
            action0.run();
        } else {
            rateLimiter.execute(action0::run);
        }
    }

    private static <T> T getResultFromFuture(final Future<T> future) {
        try {
            return future.get();
        } catch (final InterruptedException e) {
            return Utils.handleInterruptedException(e, () -> null);
        } catch (final ExecutionException e) {
            throw new UtilsException("UNKNOWN_ERROR", e);
        }
    }

    private void processMessage(final Message message, final long startTime) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(MessageFormat.format("Processing message in listener[{0}]: {1}",
                        listenerName, message));
            }
            final long timeTakenToStartProcessing = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()
                    - startTime);

            if (timeTakenToStartProcessing < CHANGE_VISIBILITY_PERIOD_IN_SECONDS) {
                final Tuple3<SqsMessage<?>, Map<String, String>, TaskInput<?>> sqsMessage;
                final String body = message.body();
                final String receiptHandle = message.receiptHandle();
                final String receiveCount = message.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT);
                final SqsMessageHandler<?> messageHandler;
                final Map<String, String> messageAttributes = message.messageAttributes().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, a -> a.getValue().stringValue()));
                final String sqsMessageWrapperPresent = messageAttributes.get(MessageConstants.SQS_MESSAGE_WRAPPER_PRESENT);

                if((StringUtils.hasLength(sqsMessageWrapperPresent) && "true".equalsIgnoreCase(sqsMessageWrapperPresent))
                        || message.body().contains("\"messageType")) {
                    sqsMessage = constructSqsMessage(body, receiptHandle);
                    messageHandler = messageHandlerFactory.getMessageHandler(sqsMessage._1(),
                            receiptHandle,
                            queueUrlFunc.apply(queueName),
                            StringUtils.hasLength(receiveCount) ? Integer.parseInt(receiveCount) : 0,
                            CollectionUtils.isEmpty(messageAttributes) ? sqsMessage._2() : messageAttributes,
                            messageHandlerRateLimiter
                    );

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(MessageFormat.format("Handling message by {0}", messageHandler));
                    }
                } else if(StringUtils.hasLength(messageAttributes.get(MessageConstants.MESSAGE_TYPE))){
                    final String messageType = messageAttributes.get(MessageConstants.MESSAGE_TYPE);
                    final String transactionId = messageAttributes.get(MessageConstants.TRANSACTION_ID);

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
            LOGGER.error(MessageFormat.format("Exception in listener[{0}]: {1}", listenerName, e.getMessage()), e);
        }
    }

    private void handleUtilsException(final Message message, final UtilsException e) {
        if ("NO_HANDLER_FOR_MESSAGE_TYPE".equalsIgnoreCase(e.getErrorType()) || "INVALID_JSON"
                .equalsIgnoreCase(e.getErrorType())) {
            LOGGER.error(MessageFormat.format("Exception in listener[{0}]: {1}", listenerName, e.getMessage()), e);
            sqsMessageClient.deleteMessage(queueUrlFunc.apply(queueName), message.receiptHandle());
        } else {
            LOGGER.error(MessageFormat.format("Exception in listener[{0}]: {1}", listenerName, e.getMessage()), e);
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
        final SnsSubscriptionMessage snsSubscriptionMessage = Utils.constructFromJson(SnsSubscriptionMessage.class, body);
        final String snsMessage = snsSubscriptionMessage.getMessage();
        final Map<String, MessageAttribute> messageAttributes = snsSubscriptionMessage.getMessageAttributes();
        final TaskInput taskInput;
        final Map<String, String> messAttr;

        if (snsMessage.contains("\"Input\"")) {
            taskInput = Utils.constructFromJson(TaskInput.class, snsMessage);

            if (taskInput.getInput() == null) {
                sqsMessageClient.deleteMessage(queueName, receiptHandle);

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
        try {
            final ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrlFunc.apply(queueName))
                    .attributeNames(QueueAttributeName.ALL)
                    .messageAttributeNames("All")
                    .maxNumberOfMessages(MAX_NUMBER_OF_SQS_MESSAGES)
                    .waitTimeSeconds(waitTimeInSeconds)
                    .build();

            return sqsAsyncClient.receiveMessage(request).get().messages();
        } catch (final InterruptedException e) {
            return Utils.handleInterruptedException(e, (Supplier<List<Message>>) Collections::emptyList);
        } catch (final ExecutionException e) {
            LOGGER.error(MessageFormat.format("Exception in receiveMessages in listener[{0}]: {1}",
                    listenerName, e), e);

            throw new UtilsException("UNKNOWN_ERROR", e);
        }
    }

    static SqsMessageListener.Builder builder() {
        return new SqsMessageListenerBuilder();
    }

    private static class SqsMessageListenerBuilder implements SqsMessageListener.Builder {
        private String queueName;
        private SqsAsyncClient sqsAsyncClient;
        private MessageHandlerFactory messageHandlerFactory;
        private SqsMessageClient sqsMessageClient;
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

        @Override
        public Builder queueName(final String queueName) {
            this.queueName = queueName;
            return this;
        }

        @Override
        public Builder queueUrl(final String queueUrl) {
            this.queueUrl = queueUrl;
            return this;
        }

        @Override
        public Builder sqsAsyncClient(final SqsAsyncClient sqsAsyncClient) {
            this.sqsAsyncClient = sqsAsyncClient;
            return this;
        }

        @Override
        public Builder messageHandlerFactory(final MessageHandlerFactory messageHandlerFactory) {
            this.messageHandlerFactory = messageHandlerFactory;
            return this;
        }

        @Override
        public Builder sqsMessageClient(final SqsMessageClient sqsMessageClient) {
            this.sqsMessageClient = sqsMessageClient;
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
        public SqsMessageListener build() {
            final SqsMessageListenerImpl sqsMessageListener = new SqsMessageListenerImpl(sqsAsyncClient,
                    queueName,
                    queueUrl, sqsMessageClient,
                    messageHandlerFactory,
                    executorService,
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
