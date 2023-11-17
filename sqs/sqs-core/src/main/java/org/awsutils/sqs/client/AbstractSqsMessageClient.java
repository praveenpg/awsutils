package org.awsutils.sqs.client;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.awsutils.common.util.Utils;
import org.awsutils.sqs.message.SqsMessage;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.services.sqs.model.*;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.awsutils.sqs.client.MessageConstants.SQS_MESSAGE_WRAPPER_PRESENT;

@Slf4j
abstract class AbstractSqsMessageClient<A, B, C, D> implements SqsMessageClient<A, B, C, D> {
    private final ConcurrentHashMap<String, String> queueUrlMap = new ConcurrentHashMap<>();

    <T> SendMessageRequest.Builder getSendMessageRequestBuilder(final T sqsMessage,
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
    <T> B validateAndSendMessage(final List<T> sqsMessages, final Supplier<B> supplier) {

        if (!CollectionUtils.isEmpty(sqsMessages) && sqsMessages.size() <= 10) {
            return supplier.get();
        } else {
            log.error(CollectionUtils.isEmpty(sqsMessages) ? "At least one message needs to be sent" : "Maximum number of messages supported is 10");
            throw new IllegalArgumentException(CollectionUtils.isEmpty(sqsMessages) ?
                    "At least one message needs to be sent" : "Maximum number of messages supported is 10");
        }
    }


    <T> B sendMessage(final List<SqsMessage<T>> sqsMessages,
                         final String queueName,
                         final Integer delayInSeconds,
                         final Map<String, String> attMap, Function<SendMessageBatchRequest, B> function) {

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

    <T> SendMessageRequest.Builder getSendMessageRequestBuilder(final SqsMessage<T> sqsMessage,
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


    <T> Map<String, MessageAttributeValue> getSqsMessageAttributeValues(final SqsMessage<T> sqsMessage,
                                                                        final Map<String, MessageAttributeValue> attributeValueMap) {

        final ImmutableMap.Builder<String, MessageAttributeValue> builder = ImmutableMap.<String, MessageAttributeValue>builder().putAll(attributeValueMap);

        if (StringUtils.hasLength(sqsMessage.getTransactionId())) {
            builder.put(MessageConstants.TRANSACTION_ID, MessageAttributeValue.builder().dataType(MessageConstants.MESSAGE_ATTRIBUTE_TYPE).stringValue(sqsMessage.getTransactionId()).build());
        }

        builder.put(MessageConstants.MESSAGE_TYPE, MessageAttributeValue.builder().dataType(MessageConstants.MESSAGE_ATTRIBUTE_TYPE).stringValue(sqsMessage.getMessageType()).build());
        return builder.build();
    }

    Map<String, MessageAttributeValue> getSqsMessageAttributeValues(final String messageType,
                                                                        final String transactionId,
                                                                        final Map<String, MessageAttributeValue> attributeValueMap) {

        final ImmutableMap.Builder<String, MessageAttributeValue> builder = ImmutableMap.<String, MessageAttributeValue>builder().putAll(attributeValueMap);

        if (StringUtils.hasLength(transactionId)) {
            builder.put(MessageConstants.TRANSACTION_ID, MessageAttributeValue.builder().dataType(MessageConstants.MESSAGE_ATTRIBUTE_TYPE).stringValue(transactionId).build());
        }

        builder.put(MessageConstants.MESSAGE_TYPE, MessageAttributeValue.builder().dataType(MessageConstants.MESSAGE_ATTRIBUTE_TYPE).stringValue(messageType).build());
        return builder.build();
    }

    Map<String, MessageAttributeValue> getSqsMessageAttributes(final Map<String, String> attMap) {
        return !CollectionUtils.isEmpty(attMap) ?
                ImmutableMap.copyOf(attMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, b -> MessageAttributeValue.builder()
                        .dataType(MessageConstants.MESSAGE_ATTRIBUTE_TYPE)
                        .stringValue(b.getValue())
                        .build())))
                : Collections.emptyMap();
    }

    <T> Map<String, String> constructFinalMessageAttributeMap(final SqsMessage<T> sqsMessage,
                                                              final Map<String, String> messageAttMap) {

        final Map<String, String> finalMessageAttributes = !CollectionUtils.isEmpty(messageAttMap) ? new HashMap<>(messageAttMap) : new HashMap<>();

        finalMessageAttributes.put(SQS_MESSAGE_WRAPPER_PRESENT, "true");

        return finalMessageAttributes;
    }

    Map<String, String> constructFinalMessageAttributeMap(final String transactionId,
                                                              final String messageType,
                                                              final Map<String, String> messageAttMap) {

        final Map<String, String> finalMessageAttributes = !CollectionUtils.isEmpty(messageAttMap) ? new HashMap<>(messageAttMap) : new HashMap<>();

        finalMessageAttributes.put(SQS_MESSAGE_WRAPPER_PRESENT, "false");

        return finalMessageAttributes;
    }

    <T> B sendMessage(final List<T> sqsMessages,
                         final String messageType,
                         final String transactionId,
                         final String queueName,
                         final Integer delayInSeconds,
                         final Map<String, String> attMap,
                         final Function<SendMessageBatchRequest, B> function) {

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

    C deleteMessage(final String queueUrl, final String receiptHandle, final Function<DeleteMessageRequest, C> function) {

        final DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();

        if (log.isDebugEnabled()) {
            log.debug(MessageFormat.format("Deleting message  from Queue: {0} with receiptHandle: {1}", queueUrl, receiptHandle));
        }
        return function.apply(deleteMessageRequest);
    }

     D changeVisibility(final String queueUrl,
                           final String receiptHandle,
                           final Integer visibilityTimeout,
                           final Function<ChangeMessageVisibilityRequest, D> function) {

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


    <T> SendMessageResponse handleSqsResponse(final SqsMessage<T> sqsMessage,
                                              final String queueName,
                                              final Integer delayInSeconds,
                                              final SendMessageResponse response) {

        if (log.isDebugEnabled()) {
            log.debug(MessageFormat.format("Sent message to {0}, message type: {1} w/ delay {2}", queueName, sqsMessage.getMessageType(), delayInSeconds));
        }

        return response;
    }

    <T> SendMessageResponse handleSqsResponse(final T sqsMessage,
                                              final String messageType,
                                              final String queueName,
                                              final Integer delayInSeconds,
                                              final SendMessageResponse response) {

        if (log.isDebugEnabled()) {
            log.debug(MessageFormat.format("Sent message to {0}, message type: {1} w/ delay {2}", queueName, messageType, delayInSeconds));
        }

        return response;
    }


    @Override
    public String getQueueUrl(final String queueName) {
        return queueUrlMap.computeIfAbsent(queueName, s -> queueUrl(queueName));
    }

    protected abstract String queueUrl(String queueName);
}
