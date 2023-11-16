package org.awsutils.sqs.handler.impl;

import org.awsutils.common.exceptions.ServiceException;
import org.awsutils.common.ratelimiter.RateLimiter;
import org.awsutils.common.util.Utils;
import org.awsutils.sqs.annotations.MessageHandler;
import org.awsutils.sqs.aspects.SqsMessageSenderInjector;
import org.awsutils.sqs.client.SyncSqsMessageClient;
import org.awsutils.sqs.handler.SqsMessageHandler;
import org.awsutils.sqs.message.SqsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import software.amazon.awssdk.utils.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public abstract class AbstractSqsMessageHandler<T> implements SqsMessageHandler<T> {
    private T message;
    private String receiptHandle;
    private String queueUrl;
    private Integer retryNumber;
    private Set<String> skipRetryForErrorTypes;
    private Set<Class<? extends Exception>> skipRetryForErrorTypesExceptions;
    private Map<String, String> messageAttributes;
    private RateLimiter rateLimiter;

    private static final int TIME_TO_PROCESS_MESSAGE_SECONDS = (int) TimeUnit.MINUTES.toSeconds(15);
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSqsMessageHandler.class);
    private String transactionId;
    @Override
    public void handle() {
        try {
            if(!StringUtils.isEmpty(transactionId)) {
                Utils.executeWithTransactionId(this::processFunction, transactionId);
            } else {
                processFunction();
            }
        } finally {
            MDC.clear();
        }
    }


    @Override
    public void handleException(T message, Throwable exception) {
        if(skipChangeVisibility(exception)) {
            LOGGER.warn("Exception received. Not retrying since error type is " + exception);
        } else {
            changeVisibility(exception);
        }
    }

    private void changeVisibility(final Throwable ex) {
        if(ex != null) {
            LOGGER.warn("Changing visibility due to error: " + ex);
        }

        final int visibilityTimeout = getVisibilityTimeout();
        changeVisibility(visibilityTimeout);
    }

    int getVisibilityTimeout() {
        return Utils.calculateVisibilityTimeout(getRetryNumber());
    }

    @Override
    public T getMessage() {
        return message;
    }

    @Override
    public void handleSuccess() {
        deleteMessage();
    }

    void deleteMessage() {
        final SqsMessageSenderInjector injector = ((SqsMessageSenderInjector) this);
        final SyncSqsMessageClient sqsMessageClient = injector.sqsMessageClient();

        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("deleting message from SQS queue: {}, Message: {}", receiptHandle, message);
        }

        if (sqsMessageClient != null) {
            sqsMessageClient.deleteMessage(queueUrl, receiptHandle);
        }
    }

    private void processFunction() {
        final Runnable processFunc = () -> {
            final T message = getMessage();
            try {
                final Object executionResult;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("In handle method of " + this);
                }
                changeVisibility(TIME_TO_PROCESS_MESSAGE_SECONDS);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Calling execute...");
                }
                executionResult = execute(message);

                if(executionResult instanceof CompletableFuture<?> future) {
                    future.whenComplete((result, e) -> handleSqsMessageProcessResult(result, e, message))
                            .exceptionallyAsync(e -> {
                                handleException(message, e);
                                return null;
                            });
                } else {
                    handleSqsMessageProcessResult(executionResult, null, message);
                }
            } catch (final Exception e) {
                LOGGER.error("Exception while handling Sqs Message: [" + this.getClass() + "]: " + e, e);
                handleException(message, e);
            }
        };

        if(rateLimiter != null) {
            rateLimiter.execute(processFunc);
        } else {
            processFunc.run();
        }
    }

    private void handleSqsMessageProcessResult(Object result, Throwable e, T message) {
        if(result != null) {
            handleSuccess();
        } else if(e != null) {
            handleException(message, e);
        } else {
            handleSuccess();
        }
    }

    private void changeVisibility(final int visibilityTimeout) {
        final SqsMessageSenderInjector injector = ((SqsMessageSenderInjector) this);
        final SyncSqsMessageClient sqsMessageClient = injector.sqsMessageClient();

        if (sqsMessageClient != null) {
            sqsMessageClient.changeVisibility(queueUrl, receiptHandle, visibilityTimeout);
        }
    }


    @SuppressWarnings("ConstantValue")
    private boolean isExceptionInstance(final Class<?> exceptionClass, final Class<? extends Exception> e) {
        if(exceptionClass != Object.class && (exceptionClass == e)) {
            return true;
        } if(exceptionClass != Object.class && (exceptionClass != e)) {
            return isExceptionInstance(exceptionClass.getSuperclass(), e);
        } else {
            return false;
        }
    }

    T validateAndReturn(final T message) {
        return message;
    }

    public int getRetryNumber() {
        return retryNumber != null ? retryNumber : 0;
    }

    protected Map<String, String> getMessageAttributes() {
        return messageAttributes;
    }

    protected String getMessageAttribute(final String key) {
        return messageAttributes.entrySet().stream()
                .filter(entry -> key.equalsIgnoreCase(entry.getKey()))
                .findFirst()
                .map(Map.Entry::getValue)
                .orElseThrow(() -> new ServiceException("NO_SUCH_ATTRIBUTE_PRESENT", "NO_SUCH_ATTRIBUTE_PRESENT"));
    }

    private boolean skipChangeVisibility(final Throwable exception) {
        return (exception instanceof ServiceException && skipRetryForErrorTypes.contains(((ServiceException) exception).getErrorType()))
                || skipRetryForErrorTypesExceptions.stream().anyMatch(e -> isExceptionInstance(exception.getClass(), e));
    }

    private void initialize(final SqsMessage<T> sqsMessage,
                            final String receiptHandle,
                            final String queueUrl,
                            final Integer retryNumber,
                            final Map<String, String> messageAttributes,
                            final RateLimiter rateLimiter) {

        final MessageHandler messageHandler;

        this.message = validateAndReturn(Utils.constructFromJson(getParameterType(), Utils.constructJson(sqsMessage.getMessage()), cause -> new ServiceException("UNKNOWN_ERROR", cause)));
        this.receiptHandle = receiptHandle;
        this.queueUrl = queueUrl;
        this.retryNumber = retryNumber;
        this.messageAttributes = messageAttributes;
        this.transactionId = sqsMessage.getTransactionId();

        messageHandler = this.getClass().getAnnotation(MessageHandler.class);
        this.skipRetryForErrorTypes = Arrays.stream(messageHandler.skipRetryFor()).collect(Collectors.toSet());
        this.skipRetryForErrorTypesExceptions = Arrays.stream(messageHandler.skipRetryForExceptions()).collect(Collectors.toSet());
        this.rateLimiter = rateLimiter;
    }

    void initialize(final String sqsMessage,
                    final String transactionId,
                    final Class<T> messageTypeClass,
                    final Method method,
                    final String receiptHandle,
                    final String queueUrl,
                    final Integer retryNumber,
                    final Map<String, String> messageAttributes,
                    final RateLimiter rateLimiter) {

        final MessageHandler messageHandler;

        this.message = validateAndReturn(Utils.constructFromJson(messageTypeClass, sqsMessage));
        this.receiptHandle = receiptHandle;
        this.queueUrl = queueUrl;
        this.retryNumber = retryNumber;
        this.messageAttributes = messageAttributes;
        this.transactionId = transactionId;

        messageHandler = method.getAnnotation(MessageHandler.class);
        this.skipRetryForErrorTypes = Arrays.stream(messageHandler.skipRetryFor()).collect(Collectors.toSet());
        this.skipRetryForErrorTypesExceptions = Arrays.stream(messageHandler.skipRetryForExceptions()).collect(Collectors.toSet());
        this.rateLimiter = rateLimiter;
    }

    @SuppressWarnings("unchecked")
    Class<T> getParameterType() {
        return (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }
}
