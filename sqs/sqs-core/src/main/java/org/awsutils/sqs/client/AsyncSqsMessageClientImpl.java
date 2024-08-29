package org.awsutils.sqs.client;

import lombok.extern.slf4j.Slf4j;
import org.awsutils.sqs.message.SqsMessage;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Async client to send SQS messages.
 */
@Slf4j
public final class AsyncSqsMessageClientImpl extends AbstractSqsMessageClient<CompletableFuture<SendMessageResponse>,
        CompletableFuture<SendMessageBatchResponse>,
        CompletableFuture<DeleteMessageResponse>,
        CompletableFuture<ChangeMessageVisibilityResponse>, CompletableFuture<GetQueueUrlResponse>> implements AsyncSqsMessageClient {
    private final SqsAsyncClient sqsAsyncClient;

    public AsyncSqsMessageClientImpl(SqsAsyncClient sqsAsyncClient) {
        this.sqsAsyncClient = sqsAsyncClient;
    }

    @Override
    public CompletableFuture<GetQueueUrlResponse> getQueueUrl(final String queueName) {

        return super.getQueueUrl(queueName, sqsAsyncClient::getQueueUrl);
    }

    @Override
    public <T> CompletableFuture<SendMessageResponse> sendMessage(final SqsMessage<T> sqsMessage,
                                                                  final String queueName,
                                                                  final Integer delayInSeconds,
                                                                  final Map<String, String> messageAttMap) {

        return super.sendSingleMessage(sqsMessage, queueName, delayInSeconds, messageAttMap, sendMessageRequest ->
                sqsAsyncClient.sendMessage(sendMessageRequest).thenApplyAsync(response -> logSqsSendResponse(sqsMessage, queueName,
                        delayInSeconds, response)));
    }

    @Override
    public <T> CompletableFuture<SendMessageResponse> sendMessage(final T sqsMessage,
                                                                  final String messageType,
                                                                  final String transactionId,
                                                                  final String queueName,
                                                                  final Integer delayInSeconds,
                                                                  final Map<String, String> messageAttMap) {

        return super.sendSingleMessage(sqsMessage, messageType, transactionId, queueName, delayInSeconds, messageAttMap,
                messageRequest -> sqsAsyncClient.sendMessage(messageRequest).thenApplyAsync(response ->
                        logSqsSendResponse(sqsMessage, messageType, queueName, delayInSeconds, response)));
    }

    @Override
    public <T> CompletableFuture<SendMessageBatchResponse> sendMessage(final List<T> sqsMessages,
                                                                       final String messageType,
                                                                       final String transactionId,
                                                                       final String queueName,
                                                                       final Integer delayInSeconds,
                                                                       final Map<String, String> messageAttMap) {

        return sendMessage(sqsMessages, messageType, transactionId, queueName, delayInSeconds, messageAttMap,
                sqsAsyncClient::sendMessageBatch);
    }

    @Override
    public <T> CompletableFuture<SendMessageBatchResponse> sendMessage(final List<SqsMessage<T>> sqsMessages,
                                                                       final String queueName,
                                                                       final Integer delayInSeconds,
                                                                       final Map<String, String> attMap) {

        return validateAndSendMessage(sqsMessages, () ->
                sendMessage(sqsMessages, queueName, delayInSeconds, attMap, sqsAsyncClient::sendMessageBatch));
    }

    @Override
    public CompletableFuture<DeleteMessageResponse> deleteMessage(final String queueUrl,
                                                                  final String receiptHandle) {

        return deleteMessage(queueUrl, receiptHandle, sqsAsyncClient::deleteMessage);
    }

    @Override
    public CompletableFuture<ChangeMessageVisibilityResponse> changeVisibility(final String queueUrl,
                                                                               final String receiptHandle,
                                                                               final Integer visibilityTimeout) {
        return changeVisibility(queueUrl, receiptHandle, visibilityTimeout, sqsAsyncClient::changeMessageVisibility);
    }
}
