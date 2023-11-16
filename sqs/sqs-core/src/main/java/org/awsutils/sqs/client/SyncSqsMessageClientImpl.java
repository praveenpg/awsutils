package org.awsutils.sqs.client;

import lombok.extern.slf4j.Slf4j;
import org.awsutils.sqs.message.SqsMessage;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;

@Slf4j
public class SyncSqsMessageClientImpl
        extends AbstractSqsMessageClient<SendMessageResponse,
                SendMessageBatchResponse,
                DeleteMessageResponse,
                ChangeMessageVisibilityResponse> implements SyncSqsMessageClient {
    private final SqsClient sqsSyncClient;

    public SyncSqsMessageClientImpl(SqsClient sqsClient) {
        this.sqsSyncClient = sqsClient;
    }

    @Override
    protected String queueUrl(String queueName) {
        final GetQueueUrlRequest queueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        final GetQueueUrlResponse queueUrlResponse = sqsSyncClient.getQueueUrl(queueUrlRequest);

        return queueUrlResponse.queueUrl();
    }

    @Override
    public <T> SendMessageResponse sendMessage(SqsMessage<T> sqsMessage, String queueName, Integer delayInSeconds, Map<String, String> messageAttMap) {
        return handleSqsResponse(sqsMessage, queueName, delayInSeconds, sqsSyncClient.sendMessage(
                getSendMessageRequestBuilder(sqsMessage, queueName, delayInSeconds, messageAttMap).build()));
    }

    @Override
    public <T> SendMessageResponse sendMessage(T sqsMessage, String messageType, String transactionId, String queueName, Integer delayInSeconds, Map<String, String> messageAttMap) {
        return handleSqsResponse(sqsMessage, messageType, queueName, delayInSeconds, sqsSyncClient.sendMessage(
                getSendMessageRequestBuilder(sqsMessage, messageType,
                        transactionId, queueName, delayInSeconds, messageAttMap).build()));
    }

    @Override
    public <T> SendMessageBatchResponse sendMessage(List<T> sqsMessages, String messageType, String transactionId, String queueName, Integer delayInSeconds, Map<String, String> attMap) {
        return sendMessage(sqsMessages, messageType, transactionId, queueName, delayInSeconds, attMap,
                sqsSyncClient::sendMessageBatch);
    }

    @Override
    public <T> SendMessageBatchResponse sendMessage(List<SqsMessage<T>> sqsMessages, String queueName, Integer delayInSeconds, Map<String, String> attMap) {
        return validateAndSendMessage(sqsMessages, () ->
                sendMessage(sqsMessages, queueName, delayInSeconds, attMap, sqsSyncClient::sendMessageBatch));
    }

    @Override
    public DeleteMessageResponse deleteMessage(String queueUrl, String receiptHandle) {
        return deleteMessage(queueUrl, receiptHandle, sqsSyncClient::deleteMessage);
    }

    @Override
    public ChangeMessageVisibilityResponse changeVisibility(String queueUrl, String receiptHandle, Integer visibilityTimeout) {
        return changeVisibility(queueUrl, receiptHandle, visibilityTimeout, sqsSyncClient::changeMessageVisibility);
    }
}
