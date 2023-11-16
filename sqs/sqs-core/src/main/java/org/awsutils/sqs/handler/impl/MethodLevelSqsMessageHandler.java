package org.awsutils.sqs.handler.impl;

import org.awsutils.common.ratelimiter.RateLimiter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class MethodLevelSqsMessageHandler<T> extends AbstractSqsMessageHandler<T> {
    private final Method method;
    private final Object handlerBean;

    public <X> MethodLevelSqsMessageHandler(String sqsMessage, String transactionId, Class parameterType, Method handlerMethod, X bean, String receiptHandle, String queueUrl, Integer retryCount, Map<String, String> messageAttributes, RateLimiter messageHandlerRateLimiter) {
        this.method = handlerMethod;
        this.handlerBean = bean;

        initializeForMethodLevelHandler(sqsMessage, transactionId, parameterType, handlerMethod, receiptHandle, queueUrl, retryCount, messageAttributes, messageHandlerRateLimiter);
    }


    @Override
    public <X> X execute(T message) {
        try {
            final var returnVal = method.invoke(handlerBean, message);

            return (X) returnVal;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw (e.getCause() instanceof RuntimeException ex ? ex: new RuntimeException(e.getCause()));
        }
    }

    public void initializeForMethodLevelHandler(final String sqsMessage,
                                                final String transactionId,
                                                final Class<T> messageTypeClass,
                                                final Method method,
                                                final String receiptHandle,
                                                final String queueUrl,
                                                final Integer retryNumber,
                                                final Map<String, String> messageAttributes,
                                                final RateLimiter rateLimiter) {

        super.initialize(sqsMessage, transactionId, messageTypeClass, method, receiptHandle, queueUrl, retryNumber, messageAttributes, rateLimiter);
    }

    @Override
    Class<T> getParameterType() {
        return (Class<T>) method.getParameterTypes()[1];
    }
}
