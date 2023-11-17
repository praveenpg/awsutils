package org.awsutils.sqs.client;

import lombok.extern.slf4j.Slf4j;
import org.awsutils.sqs.message.SqsMessage;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;

@Slf4j
public final class SyncSqsMessageClientImpl
        extends AbstractSqsMessageClient<SendMessageResponse,
        SendMessageBatchResponse,
        DeleteMessageResponse,
        ChangeMessageVisibilityResponse> implements SyncSqsMessageClient {
    private final SqsClient sqsSyncClient;

    public SyncSqsMessageClientImpl(final SqsClient sqsClient) {
        this.sqsSyncClient = sqsClient;
    }

    @Override
    protected String queueUrl(final String queueName) {
        final GetQueueUrlRequest queueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        final GetQueueUrlResponse queueUrlResponse = sqsSyncClient.getQueueUrl(queueUrlRequest);

        return queueUrlResponse.queueUrl();
    }

    @Override
    public <T> SendMessageResponse sendMessage(final SqsMessage<T> sqsMessage,
                                               final String queueName,
                                               final Integer delayInSeconds,
                                               final Map<String, String> messageAttMap) {

        return handleSqsResponse(sqsMessage, queueName, delayInSeconds, sqsSyncClient.sendMessage(
                getSendMessageRequestBuilder(sqsMessage, queueName, delayInSeconds, messageAttMap).build()));
    }

    @Override
    public <T> SendMessageResponse sendMessage(final T sqsMessage,
                                               final String messageType,
                                               final String transactionId,
                                               final String queueName,
                                               final Integer delayInSeconds,
                                               final Map<String, String> messageAttMap) {

        return handleSqsResponse(sqsMessage, messageType, queueName, delayInSeconds, sqsSyncClient.sendMessage(
                getSendMessageRequestBuilder(sqsMessage, messageType,
                        transactionId, queueName, delayInSeconds, messageAttMap).build()));
    }

    @Override
    public <T> SendMessageBatchResponse sendMessage(final List<T> sqsMessages,
                                                    final String messageType,
                                                    final String transactionId,
                                                    final String queueName,
                                                    final Integer delayInSeconds,
                                                    final Map<String, String> attMap) {

        return sendMessage(sqsMessages, messageType, transactionId, queueName, delayInSeconds, attMap,
                sqsSyncClient::sendMessageBatch);
    }

    @Override
    public <T> SendMessageBatchResponse sendMessage(final List<SqsMessage<T>> sqsMessages,
                                                    final String queueName,
                                                    final Integer delayInSeconds,
                                                    final Map<String, String> attMap) {

        return validateAndSendMessage(sqsMessages, () ->
                sendMessage(sqsMessages, queueName, delayInSeconds, attMap, sqsSyncClient::sendMessageBatch));
    }

    @Override
    public DeleteMessageResponse deleteMessage(final String queueUrl,
                                               final String receiptHandle) {
        return deleteMessage(queueUrl, receiptHandle, sqsSyncClient::deleteMessage);
    }

    @Override
    public ChangeMessageVisibilityResponse changeVisibility(final String queueUrl,
                                                            final String receiptHandle,
                                                            final Integer visibilityTimeout) {
        return changeVisibility(queueUrl, receiptHandle, visibilityTimeout, sqsSyncClient::changeMessageVisibility);
    }
}
