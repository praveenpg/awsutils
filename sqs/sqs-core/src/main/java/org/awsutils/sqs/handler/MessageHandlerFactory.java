package org.awsutils.sqs.handler;





import org.awsutils.sqs.message.SqsMessage;
import org.awsutils.sqs.ratelimiter.RateLimiter;

import java.util.Map;

public interface MessageHandlerFactory {
    <T> SqsMessageHandler<T> getMessageHandler(String sqsMessage,
                                               String messageType, String transactionId, String receiptHandle,
                                               String queueUrl,
                                               Integer retryCount,
                                               Map<String, String> messageAttributes,
                                               RateLimiter messageHandlerRateLimiter);

    <T> SqsMessageHandler<T> getMessageHandler(final SqsMessage<T> sqsMessage, final String receiptHandle, final String queueUrl, final Integer retryNumber, final Map<String, String> messageAttributes, final RateLimiter messageHandlerRateLimiter);
}
