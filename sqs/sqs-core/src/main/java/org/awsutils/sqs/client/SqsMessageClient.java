package org.awsutils.sqs.client;

import org.awsutils.sqs.message.SqsBatchMessage;
import org.awsutils.sqs.message.SqsMessage;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface SqsMessageClient<A, B, C, D> {
    default <T> A sendMessage(final T message, String messageType, String transactionId, final String queueName) {
        return sendMessage(message, messageType, transactionId, queueName,  BigInteger.ZERO.intValue(), Collections.emptyMap());
    }
    default <T> A sendMessage(final SqsMessage<T> sqsMessage, final String queueName) {
        return sendMessage(sqsMessage, queueName, BigInteger.ZERO.intValue(), Collections.emptyMap());
    }

    default <T> A sendMessage(final T message, String messageType, String transactionId, final String queueName, final Integer delayInSeconds) {
        return sendMessage(message, messageType, transactionId, queueName, delayInSeconds, Collections.emptyMap());
    }

    default <T> A sendMessage(final SqsMessage<T> sqsMessage, final String queueName, final Integer delayInSeconds) {
        return sendMessage(sqsMessage, queueName, delayInSeconds, Collections.emptyMap());
    }

    default <T> B sendMessage(final List<T> message, String messageType, String transactionId, final String queueName) {
        return sendMessage(message, messageType, transactionId, queueName, BigInteger.ZERO.intValue());
    }

    default <T> B sendMessage(final SqsBatchMessage<T> sqsBatchMessage, final String queueName) {
        return sendMessage(sqsBatchMessage, queueName, BigInteger.ZERO.intValue());
    }


    default <T> B sendMessage(final List<T> message, String messageType, String transactionId, final String queueName, final Integer delayInSeconds) {
        return sendMessage(message, messageType, transactionId, queueName, delayInSeconds, Collections.emptyMap());
    }
    default <T> B sendMessage(final SqsBatchMessage<T> sqsBatchMessage, final String queueName, final Integer delayInSeconds) {
        return sendMessage(sqsBatchMessage, queueName, delayInSeconds, Collections.emptyMap());
    }

    default <T> B sendMessage(final List<SqsMessage<T>> sqsMessages, final String queueName) {
        return sendMessage(sqsMessages, queueName, 0);
    }

    <T> A sendMessage(SqsMessage<T> sqsMessage, String queueName, Integer delayInSeconds, Map<String, String> messageAttMap);

    <T> A sendMessage(T sqsMessage, String messageType, String transactionId, String queueName, Integer delayInSeconds, Map<String, String> messageAttMap);

    default <T> B sendMessage(final List<SqsMessage<T>> sqsMessage, final String queueName, final Integer delayInSeconds) {
        return sendMessage(sqsMessage, queueName, delayInSeconds, Collections.emptyMap());
    }
    
    <T> B sendMessage(List<T> sqsMessages, String messageType, String transactionId, String queueName, Integer delayInSeconds, Map<String, String> attMap);

    <T> B sendMessage(List<SqsMessage<T>> sqsMessages, String queueName, Integer delayInSeconds, Map<String, String> attMap);

    String getQueueUrl(String queueName);

    C deleteMessage(String queueUrl, String receiptHandle);

    D changeVisibility(String queueUrl, String receiptHandle, Integer visibilityTimeout);

    default <T> B sendMessage(final SqsBatchMessage<T> sqsBatchMessage, final String queueName, final Integer delayInSeconds, Map<String, String> attMap) {
        return sendMessage(sqsBatchMessage.sqsMessages(), queueName, delayInSeconds, attMap);
    }
}
