package org.awsutils.sqs.client;

import org.awsutils.sqs.message.SqsBatchMessage;
import org.awsutils.sqs.message.SqsMessage;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface SqsMessageClient {
    default <T> CompletableFuture<SendMessageResponse> sendMessage(final T message, String messageType, String transactionId, final String queueName) {
        return sendMessage(message, messageType, transactionId, queueName,  BigInteger.ZERO.intValue(), Collections.emptyMap());
    }
    default <T> CompletableFuture<SendMessageResponse> sendMessage(final SqsMessage<T> sqsMessage, final String queueName) {
        return sendMessage(sqsMessage, queueName, BigInteger.ZERO.intValue(), Collections.emptyMap());
    }

    default <T> CompletableFuture<SendMessageResponse> sendMessage(final T message, String messageType, String transactionId, final String queueName, final Integer delayInSeconds) {
        return sendMessage(message, messageType, transactionId, queueName, delayInSeconds, Collections.emptyMap());
    }

    default <T> CompletableFuture<SendMessageResponse> sendMessage(final SqsMessage<T> sqsMessage, final String queueName, final Integer delayInSeconds) {
        return sendMessage(sqsMessage, queueName, delayInSeconds, Collections.emptyMap());
    }

    default <T> CompletableFuture<SendMessageBatchResponse> sendMessage(final List<T> message, String messageType, String transactionId, final String queueName) {
        return sendMessage(message, messageType, transactionId, queueName, BigInteger.ZERO.intValue());
    }

    default <T> CompletableFuture<SendMessageBatchResponse> sendMessage(final SqsBatchMessage<T> sqsBatchMessage, final String queueName) {
        return sendMessage(sqsBatchMessage, queueName, BigInteger.ZERO.intValue());
    }


    default <T> CompletableFuture<SendMessageBatchResponse> sendMessage(final List<T> message, String messageType, String transactionId, final String queueName, final Integer delayInSeconds) {
        return sendMessage(message, messageType, transactionId, queueName, delayInSeconds, Collections.emptyMap());
    }
    default <T> CompletableFuture<SendMessageBatchResponse> sendMessage(final SqsBatchMessage<T> sqsBatchMessage, final String queueName, final Integer delayInSeconds) {
        return sendMessage(sqsBatchMessage, queueName, delayInSeconds, Collections.emptyMap());
    }

    default <T> CompletableFuture<SendMessageBatchResponse> sendMessage(final List<SqsMessage<T>> sqsMessages, final String queueName) {
        return sendMessage(sqsMessages, queueName, 0);
    }

    <T> CompletableFuture<SendMessageResponse> sendMessage(SqsMessage<T> sqsMessage, String queueName, Integer delayInSeconds, Map<String, String> messageAttMap);

    <T> CompletableFuture<SendMessageResponse> sendMessage(T sqsMessage, String messageType, String transactionId, String queueName, Integer delayInSeconds, Map<String, String> messageAttMap);

    default <T> CompletableFuture<SendMessageBatchResponse> sendMessage(final List<SqsMessage<T>> sqsMessage, final String queueName, final Integer delayInSeconds) {
        return sendMessage(sqsMessage, queueName, delayInSeconds, Collections.emptyMap());
    }

    <T> CompletableFuture<SendMessageBatchResponse> sendMessage(List<T> sqsMessages, String messageType, String transactionId, String queueName, Integer delayInSeconds, Map<String, String> attMap);
    <T> CompletableFuture<SendMessageBatchResponse> sendMessage(List<SqsMessage<T>> sqsMessages, String queueName, Integer delayInSeconds, Map<String, String> attMap);

    String getQueueUrl(String queueName);

    CompletableFuture<DeleteMessageResponse> deleteMessage(String queueUrl, String receiptHandle);

    CompletableFuture<ChangeMessageVisibilityResponse> changeVisibility(String queueUrl, String receiptHandle, Integer visibilityTimeout);

    default <T> CompletableFuture<SendMessageBatchResponse> sendMessage(final SqsBatchMessage<T> sqsBatchMessage, final String queueName, final Integer delayInSeconds, Map<String, String> attMap) {
        return sendMessage(sqsBatchMessage.sqsMessages(), queueName, delayInSeconds, attMap);
    }
}
