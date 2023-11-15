package org.awsutils.sqs.client;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.awsutils.common.exceptions.UtilsException;
import org.awsutils.common.util.Utils;
import org.awsutils.sqs.message.SqsMessage;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.awsutils.sqs.client.MessageConstants.SQS_MESSAGE_WRAPPER_PRESENT;

@SuppressWarnings("unused")
@Slf4j
public class SqsMessageClientImpl implements SqsMessageClient {
    private final SqsAsyncClient sqsAsyncClient;

    private final SqsClient sqsSyncClient;
    private final ConcurrentHashMap<String, String> queueUrlMap = new ConcurrentHashMap<>();
    private static final int MAX_NUMBER_OF_MESSAGES = 10;

    public SqsMessageClientImpl(final SqsAsyncClient sqsAsyncClient, SqsClient sqsSyncClient) {
        this.sqsAsyncClient = sqsAsyncClient;
        this.sqsSyncClient = sqsSyncClient;
    }

    @Override
    public <T> CompletableFuture<SendMessageResponse> sendMessage(T sqsMessage, String messageType, String transactionId, String queueName, Integer delayInSeconds, Map<String, String> messageAttMap) {

        return sqsAsyncClient.sendMessage(getSendMessageRequestBuilder(sqsMessage, messageType, transactionId, queueName,
                        delayInSeconds, messageAttMap).build())
                .thenApplyAsync(response -> handleSqsResponse(
                        sqsMessage, messageType, queueName, delayInSeconds, response));
    }

    @Override
    public <T> SendMessageResponse sendMessageSync(T sqsMessage, String messageType, String transactionId, String queueName, Integer delayInSeconds, Map<String, String> messageAttMap) {
        return handleSqsResponse(sqsMessage, messageType, queueName, delayInSeconds, sqsSyncClient.sendMessage(
                getSendMessageRequestBuilder(sqsMessage, messageType,
                        transactionId, queueName, delayInSeconds, messageAttMap).build()));
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
    public <T> SendMessageResponse sendMessageSync(final SqsMessage<T> sqsMessage,
                                                   final String queueName,
                                                   final Integer delayInSeconds,
                                                   final Map<String, String> messageAttMap) {


        return handleSqsResponse(sqsMessage, queueName, delayInSeconds, sqsSyncClient.sendMessage(
                getSendMessageRequestBuilder(sqsMessage, queueName, delayInSeconds, messageAttMap).build()));
    }


    @Override
    public <T> CompletableFuture<SendMessageBatchResponse> sendMessage(List<T> sqsMessages, String messageType, String transactionId, String queueName, Integer delayInSeconds, Map<String, String> attMap) {

        return sendMessage(sqsMessages, messageType, transactionId, queueName, delayInSeconds, attMap,
                sqsAsyncClient::sendMessageBatch);
    }

    @Override
    public <T> SendMessageBatchResponse sendMessageSync(List<T> sqsMessages,
                                                        String messageType,
                                                        String transactionId,
                                                        String queueName,
                                                        Integer delayInSeconds,
                                                        Map<String, String> attMap) {

        return sendMessage(sqsMessages, messageType, transactionId, queueName, delayInSeconds, attMap,
                sqsSyncClient::sendMessageBatch);
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
    public <T> SendMessageBatchResponse sendMessageSync(final List<SqsMessage<T>> sqsMessages,
                                                        final String queueName,
                                                        final Integer delayInSeconds,
                                                        final Map<String, String> attMap) {

        return validateAndSendMessage(sqsMessages, () ->
                sendMessage(sqsMessages, queueName, delayInSeconds, attMap, sqsSyncClient::sendMessageBatch));
    }


    @Override
    public String getQueueUrl(final String queueName) {
        return queueUrlMap.computeIfAbsent(queueName, s -> queueUrl(queueName));
    }

    @Override
    public CompletableFuture<DeleteMessageResponse> deleteMessage(final String queueUrl,
                                                                  final String receiptHandle) {

        return deleteMessage(queueUrl, receiptHandle, sqsAsyncClient::deleteMessage);
    }

    @Override
    public DeleteMessageResponse deleteMessageSync(final String queueUrl,
                                                   final String receiptHandle) {

        final DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();

        if (log.isDebugEnabled()) {
            log.debug(MessageFormat.format("Deleting message  from Queue: {0} with receiptHandle: {1}", queueUrl, receiptHandle));
        }
        return deleteMessage(queueUrl, receiptHandle, sqsSyncClient::deleteMessage);
    }


    @Override
    public CompletableFuture<ChangeMessageVisibilityResponse> changeVisibility(final String queueUrl,
                                                                               final String receiptHandle,
                                                                               final Integer visibilityTimeout) {

        return changeVisibility(queueUrl, receiptHandle, visibilityTimeout, sqsAsyncClient::changeMessageVisibility);
    }

    @Override
    public ChangeMessageVisibilityResponse changeVisibilitySync(final String queueUrl,
                                                                final String receiptHandle,
                                                                final Integer visibilityTimeout) {

        return changeVisibility(queueUrl, receiptHandle, visibilityTimeout, sqsSyncClient::changeMessageVisibility);
    }

    private <T> SendMessageRequest.Builder getSendMessageRequestBuilder(final T sqsMessage,
                                                                        final String messageType,
                                                                        final String transactionId,
                                                                        final String queueName,
                                                                        final Integer delayInSeconds,
                                                                        final Map<String, String> messageAttMap) {

        final String finalMessage = sqsMessage instanceof String str ? str : Utils.constructJson(sqsMessage);
        final SendMessageRequest.Builder sendMessageRequestBuilder = SendMessageRequest.builder()
                .messageBody(finalMessage)
                .delaySeconds(delayInSeconds)
                .queueUrl(getQueueUrl(queueName));
        final Map<String, String> finalMessageAttributes = !CollectionUtils.isEmpty(messageAttMap) ? new HashMap<>(messageAttMap) : new HashMap<>();

        finalMessageAttributes.put(SQS_MESSAGE_WRAPPER_PRESENT, "false");

        sendMessageRequestBuilder.messageAttributes(getSqsMessageAttributeValues(messageType, transactionId,
                getSqsMessageAttributes(finalMessageAttributes)));

        if (log.isInfoEnabled()) {
            log.info(MessageFormat.format("Sending message to SQS [{0}]: {1}", getQueueUrl(queueName), sqsMessage));
        }

        return sendMessageRequestBuilder;
    }

    private <T> SendMessageRequest.Builder getSendMessageRequestBuilder(final SqsMessage<T> sqsMessage,
                                                                        final String queueName,
                                                                        final Integer delayInSeconds,
                                                                        final Map<String, String> messageAttMap) {

        final SendMessageRequest.Builder sendMessageRequestBuilder = SendMessageRequest.builder()
                .messageBody(Utils.constructJson(sqsMessage))
                .delaySeconds(delayInSeconds)
                .queueUrl(getQueueUrl(queueName));
        final Map<String, String> finalMessageAttributes = !CollectionUtils.isEmpty(messageAttMap) ? new HashMap<>(messageAttMap) : new HashMap<>();

        finalMessageAttributes.put(SQS_MESSAGE_WRAPPER_PRESENT, "true");

        sendMessageRequestBuilder.messageAttributes(getSqsMessageAttributeValues(sqsMessage, getSqsMessageAttributes(finalMessageAttributes)));

        if (log.isInfoEnabled()) {
            log.info(MessageFormat.format("Sending message to SQS [{0}]: {1}", getQueueUrl(queueName), sqsMessage));
        }
        return sendMessageRequestBuilder;
    }

    private static <T> Map<String, String> constructFinalMessageAttributeMap(final SqsMessage<T> sqsMessage,
                                                                             final Map<String, String> messageAttMap) {

        final Map<String, String> finalMessageAttributes = !CollectionUtils.isEmpty(messageAttMap) ? new HashMap<>(messageAttMap) : new HashMap<>();

        finalMessageAttributes.put(SQS_MESSAGE_WRAPPER_PRESENT, "true");

        return finalMessageAttributes;
    }

    private static <T> Map<String, String> constructFinalMessageAttributeMap(final String transactionId,
                                                                             final String messageType,
                                                                             final Map<String, String> messageAttMap) {

        final Map<String, String> finalMessageAttributes = !CollectionUtils.isEmpty(messageAttMap) ? new HashMap<>(messageAttMap) : new HashMap<>();

        finalMessageAttributes.put(SQS_MESSAGE_WRAPPER_PRESENT, "false");

        return finalMessageAttributes;
    }

    private <T, A> A sendMessage(final List<T> sqsMessages,
                                 final String messageType,
                                 final String transactionId,
                                 final String queueName,
                                 final Integer delayInSeconds,
                                 final Map<String, String> attMap,
                                 final Function<SendMessageBatchRequest, A> function) {

        return validateAndSendMessage(sqsMessages, () -> {
            final Map<String, MessageAttributeValue> attributeValueMap = getSqsMessageAttributes(constructFinalMessageAttributeMap(transactionId, messageType, attMap));
            final String queueUrl = getQueueUrl(queueName);
            final SendMessageBatchRequest request = SendMessageBatchRequest.builder().entries(
                            sqsMessages.stream().map(sqsMessage -> SendMessageBatchRequestEntry
                                    .builder()
                                    .id(StringUtils.hasLength(transactionId) ? transactionId : UUID.randomUUID().toString())
                                    .messageBody(Utils.constructJson(sqsMessage))
                                    .delaySeconds(delayInSeconds)
                                    .messageAttributes(getSqsMessageAttributeValues(messageType, transactionId, attributeValueMap))
                                    .build()).collect(Collectors.toList()))
                    .queueUrl(queueUrl)
                    .build();

            if (log.isDebugEnabled()) {
                log.debug(MessageFormat.format("Sending messages to SQS[{0}] : {1}", queueUrl, sqsMessages));
            }

            return function.apply(request);
        });
    }

    private <T, A> A validateAndSendMessage(final List<T> sqsMessages, final Supplier<A> supplier) {

        if (!CollectionUtils.isEmpty(sqsMessages) && sqsMessages.size() <= 10) {
            return supplier.get();
        } else {
            log.error(CollectionUtils.isEmpty(sqsMessages) ? "At least one message needs to be sent" : "Maximum number of messages supported is 10");
            throw new IllegalArgumentException(CollectionUtils.isEmpty(sqsMessages) ?
                    "At least one message needs to be sent" : "Maximum number of messages supported is 10");
        }
    }

    private <T, A> A sendMessage(final List<SqsMessage<T>> sqsMessages,
                                 final String queueName,
                                 final Integer delayInSeconds,
                                 final Map<String, String> attMap, Function<SendMessageBatchRequest, A> function) {

        return validateAndSendMessage(sqsMessages, () -> {
            final Map<String, MessageAttributeValue> attributeValueMap = getSqsMessageAttributes(constructFinalMessageAttributeMap(sqsMessages.get(0), attMap));
            final String queueUrl = getQueueUrl(queueName);
            final Set<String> uniqueTransactionIds = sqsMessages.stream()
                    .map(SqsMessage::getTransactionId)
                    .filter(StringUtils::hasLength)
                    .collect(Collectors.toSet());
            final boolean areTransactionIdsUnique = !CollectionUtils.isEmpty(uniqueTransactionIds) && (uniqueTransactionIds.size() == sqsMessages.size());
            final SendMessageBatchRequest request = SendMessageBatchRequest.builder().entries(
                            sqsMessages.stream().map(sqsMessage -> SendMessageBatchRequestEntry
                                    .builder()
                                    .id(areTransactionIdsUnique ? sqsMessage.getTransactionId() : UUID.randomUUID().toString())
                                    .messageBody(Utils.constructJson(sqsMessage))
                                    .delaySeconds(delayInSeconds)
                                    .messageAttributes(getSqsMessageAttributeValues(sqsMessage, attributeValueMap))
                                    .build()).collect(Collectors.toList()))
                    .queueUrl(queueUrl)
                    .build();

            if (log.isDebugEnabled()) {
                log.debug(MessageFormat.format("Sending messages to SQS[{0}] : {1}", queueUrl, sqsMessages));
            }

            return function.apply(request);
        });
    }

    private <A> A deleteMessage(final String queueUrl, final String receiptHandle, final Function<DeleteMessageRequest, A> function) {

        final DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();

        if (log.isDebugEnabled()) {
            log.debug(MessageFormat.format("Deleting message  from Queue: {0} with receiptHandle: {1}", queueUrl, receiptHandle));
        }
        return function.apply(deleteMessageRequest);
    }

    private <A> A changeVisibility(final String queueUrl,
                                   final String receiptHandle,
                                   final Integer visibilityTimeout,
                                   final Function<ChangeMessageVisibilityRequest, A> function) {

        final ChangeMessageVisibilityRequest request = ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .visibilityTimeout(visibilityTimeout)
                .build();

        if (log.isDebugEnabled()) {
            log.debug(MessageFormat.format("Changing visibility of [{0}] from queue: {1}", receiptHandle, queueUrl));
        }

        return function.apply(request);
    }

    private Map<String, MessageAttributeValue> getSqsMessageAttributes(final Map<String, String> attMap) {
        return !CollectionUtils.isEmpty(attMap) ?
                ImmutableMap.copyOf(attMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, b -> MessageAttributeValue.builder()
                        .dataType(MessageConstants.MESSAGE_ATTRIBUTE_TYPE)
                        .stringValue(b.getValue())
                        .build())))
                : Collections.emptyMap();
    }

    private <T> Map<String, MessageAttributeValue> getSqsMessageAttributeValues(final SqsMessage<T> sqsMessage,
                                                                                final Map<String, MessageAttributeValue> attributeValueMap) {

        final ImmutableMap.Builder<String, MessageAttributeValue> builder = ImmutableMap.<String, MessageAttributeValue>builder().putAll(attributeValueMap);

        if (StringUtils.hasLength(sqsMessage.getTransactionId())) {
            builder.put(MessageConstants.TRANSACTION_ID, MessageAttributeValue.builder().dataType(MessageConstants.MESSAGE_ATTRIBUTE_TYPE).stringValue(sqsMessage.getTransactionId()).build());
        }

        builder.put(MessageConstants.MESSAGE_TYPE, MessageAttributeValue.builder().dataType(MessageConstants.MESSAGE_ATTRIBUTE_TYPE).stringValue(sqsMessage.getMessageType()).build());
        return builder.build();
    }

    private <T> Map<String, MessageAttributeValue> getSqsMessageAttributeValues(final String messageType,
                                                                                final String transactionId,
                                                                                final Map<String, MessageAttributeValue> attributeValueMap) {

        final ImmutableMap.Builder<String, MessageAttributeValue> builder = ImmutableMap.<String, MessageAttributeValue>builder().putAll(attributeValueMap);

        if (StringUtils.hasLength(transactionId)) {
            builder.put(MessageConstants.TRANSACTION_ID, MessageAttributeValue.builder().dataType(MessageConstants.MESSAGE_ATTRIBUTE_TYPE).stringValue(transactionId).build());
        }

        builder.put(MessageConstants.MESSAGE_TYPE, MessageAttributeValue.builder().dataType(MessageConstants.MESSAGE_ATTRIBUTE_TYPE).stringValue(messageType).build());
        return builder.build();
    }

    private String queueUrl(final String queueName) {
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

    private <T> SendMessageResponse handleSqsResponse(final SqsMessage<T> sqsMessage,
                                                      final String queueName,
                                                      final Integer delayInSeconds,
                                                      final SendMessageResponse response) {

        if (log.isDebugEnabled()) {
            log.debug(MessageFormat.format("Sent message to {0}, message type: {1} w/ delay {2}", queueName, sqsMessage.getMessageType(), delayInSeconds));
        }

        return response;
    }

    private <T> SendMessageResponse handleSqsResponse(final T sqsMessage,
                                                      final String messageType,
                                                      final String queueName,
                                                      final Integer delayInSeconds,
                                                      final SendMessageResponse response) {

        if (log.isDebugEnabled()) {
            log.debug(MessageFormat.format("Sent message to {0}, message type: {1} w/ delay {2}", queueName, messageType, delayInSeconds));
        }

        return response;
    }
}
