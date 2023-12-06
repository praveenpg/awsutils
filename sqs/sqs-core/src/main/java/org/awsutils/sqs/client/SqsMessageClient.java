package org.awsutils.sqs.client;

import org.awsutils.sqs.message.SqsBatchMessage;
import org.awsutils.sqs.message.SqsMessage;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unused", "UnusedReturnValue"})
public interface SqsMessageClient<SEND_MSG_RESP_TYPE, SEND_BATCH_MSG_RESP_TYPE, DELETE_MSG_RESP_TYPE, CHANGE_VSB_RESP_TYPE, GET_QUEUE_URL_RESPONSE> {
    default <T> SEND_MSG_RESP_TYPE sendMessage(final T message, String messageType, String transactionId, final String queueName) {
        return sendMessage(message, messageType, transactionId, queueName,  BigInteger.ZERO.intValue(), Collections.emptyMap());
    }
    default <T> SEND_MSG_RESP_TYPE sendMessage(final SqsMessage<T> sqsMessage, final String queueName) {
        return sendMessage(sqsMessage, queueName, BigInteger.ZERO.intValue(), Collections.emptyMap());
    }

    default <T> SEND_MSG_RESP_TYPE sendMessage(final T message, String messageType, String transactionId, final String queueName, final Integer delayInSeconds) {
        return sendMessage(message, messageType, transactionId, queueName, delayInSeconds, Collections.emptyMap());
    }

    default <T> SEND_MSG_RESP_TYPE sendMessage(final SqsMessage<T> sqsMessage, final String queueName, final Integer delayInSeconds) {
        return sendMessage(sqsMessage, queueName, delayInSeconds, Collections.emptyMap());
    }

    default <T> SEND_BATCH_MSG_RESP_TYPE sendMessage(final List<T> message, String messageType, String transactionId, final String queueName) {
        return sendMessage(message, messageType, transactionId, queueName, BigInteger.ZERO.intValue());
    }

    default <T> SEND_BATCH_MSG_RESP_TYPE sendMessage(final SqsBatchMessage<T> sqsBatchMessage, final String queueName) {
        return sendMessage(sqsBatchMessage, queueName, BigInteger.ZERO.intValue());
    }


    default <T> SEND_BATCH_MSG_RESP_TYPE sendMessage(final List<T> message, String messageType, String transactionId, final String queueName, final Integer delayInSeconds) {
        return sendMessage(message, messageType, transactionId, queueName, delayInSeconds, Collections.emptyMap());
    }
    default <T> SEND_BATCH_MSG_RESP_TYPE sendMessage(final SqsBatchMessage<T> sqsBatchMessage, final String queueName, final Integer delayInSeconds) {
        return sendMessage(sqsBatchMessage, queueName, delayInSeconds, Collections.emptyMap());
    }

    default <T> SEND_BATCH_MSG_RESP_TYPE sendMessage(final List<SqsMessage<T>> sqsMessages, final String queueName) {
        return sendMessage(sqsMessages, queueName, 0);
    }

    <T> SEND_MSG_RESP_TYPE sendMessage(SqsMessage<T> sqsMessage, String queueName, Integer delayInSeconds, Map<String, String> messageAttMap);

    <T> SEND_MSG_RESP_TYPE sendMessage(T sqsMessage, String messageType, String transactionId, String queueName, Integer delayInSeconds, Map<String, String> messageAttMap);

    default <T> SEND_BATCH_MSG_RESP_TYPE sendMessage(final List<SqsMessage<T>> sqsMessage, final String queueName, final Integer delayInSeconds) {
        return sendMessage(sqsMessage, queueName, delayInSeconds, Collections.emptyMap());
    }
    
    <T> SEND_BATCH_MSG_RESP_TYPE sendMessage(List<T> sqsMessages, String messageType, String transactionId, String queueName, Integer delayInSeconds, Map<String, String> attMap);

    <T> SEND_BATCH_MSG_RESP_TYPE sendMessage(List<SqsMessage<T>> sqsMessages, String queueName, Integer delayInSeconds, Map<String, String> attMap);

    DELETE_MSG_RESP_TYPE deleteMessage(String queueUrl, String receiptHandle);

    CHANGE_VSB_RESP_TYPE changeVisibility(String queueUrl, String receiptHandle, Integer visibilityTimeout);

    default <T> SEND_BATCH_MSG_RESP_TYPE sendMessage(final SqsBatchMessage<T> sqsBatchMessage, final String queueName, final Integer delayInSeconds, Map<String, String> attMap) {
        return sendMessage(sqsBatchMessage.sqsMessages(), queueName, delayInSeconds, attMap);
    }

    GET_QUEUE_URL_RESPONSE getQueueUrl(String queueName);
}
