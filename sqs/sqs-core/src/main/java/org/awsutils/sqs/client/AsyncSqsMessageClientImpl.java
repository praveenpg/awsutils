package org.awsutils.sqs.client;

import lombok.extern.slf4j.Slf4j;
import org.awsutils.common.exceptions.UtilsException;
import org.awsutils.sqs.message.SqsMessage;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
public final class AsyncSqsMessageClientImpl extends AbstractSqsMessageClient<CompletableFuture<SendMessageResponse>,
        CompletableFuture<SendMessageBatchResponse>,
        CompletableFuture<DeleteMessageResponse>,
        CompletableFuture<ChangeMessageVisibilityResponse>> implements AsyncSqsMessageClient {
    private final SqsAsyncClient sqsAsyncClient;

    public AsyncSqsMessageClientImpl(SqsAsyncClient sqsAsyncClient) {
        this.sqsAsyncClient = sqsAsyncClient;
    }


    @Override
    protected String queueUrl(final String queueName) {
        try {
            final GetQueueUrlRequest queueUrlRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            final CompletableFuture<GetQueueUrlResponse> queueUrlResponseFut = sqsAsyncClient.getQueueUrl(queueUrlRequest);
            final GetQueueUrlResponse queueUrlResponse = queueUrlResponseFut.get();

            return queueUrlResponse.queueUrl();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UtilsException("UNKNOWN_ERROR", e);
        } catch (final ExecutionException e) {
            log.error("Exception while getting queueUrl [ " + queueName + "]: " + e, e.getCause());
            throw new UtilsException("UNKNOWN_ERROR", MessageFormat.format("Exception while getting queueUrl [ {0}]: ", queueName), e.getCause());
        }
    }

    @Override
    public <T> CompletableFuture<SendMessageResponse> sendMessage(final SqsMessage<T> sqsMessage,
                                                                  final String queueName,
                                                                  final Integer delayInSeconds,
                                                                  final Map<String, String> messageAttMap) {

        return sqsAsyncClient.sendMessage(getSendMessageRequestBuilder(sqsMessage, queueName, delayInSeconds, messageAttMap).build())
                .thenApplyAsync(response -> handleSqsResponse(sqsMessage, queueName, delayInSeconds, response));
    }

    @Override
    public <T> CompletableFuture<SendMessageResponse> sendMessage(final T sqsMessage,
                                                                  final String messageType,
                                                                  final String transactionId,
                                                                  final String queueName,
                                                                  final Integer delayInSeconds,
                                                                  final Map<String, String> messageAttMap) {

        return sqsAsyncClient.sendMessage(getSendMessageRequestBuilder(sqsMessage, messageType, transactionId, queueName,
                        delayInSeconds, messageAttMap).build())
                .thenApplyAsync(response -> handleSqsResponse(
                        sqsMessage, messageType, queueName, delayInSeconds, response));
    }

    @Override
    public <T> CompletableFuture<SendMessageBatchResponse> sendMessage(final List<T> sqsMessages,
                                                                       final String messageType,
                                                                       final String transactionId,
                                                                       final String queueName,
                                                                       final Integer delayInSeconds,
                                                                       final Map<String, String> attMap) {

        return sendMessage(sqsMessages, messageType, transactionId, queueName, delayInSeconds, attMap,
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