package org.awsutils.sqs.client;


import org.awsutils.sqs.message.SnsMessage;
import org.awsutils.sqs.util.Tuple2;
import org.awsutils.sqs.util.Utils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class SnsServiceImpl implements SnsService {
    private final SnsAsyncClient snsAsyncClient;
    private static final String STRING_DATA_TYPE = "String";

    public SnsServiceImpl(final SnsAsyncClient snsAsyncClient) {
        this.snsAsyncClient = snsAsyncClient;
    }

    @Override
    public <T> CompletableFuture<PublishResponse> publishMessage(final SnsMessage<T> snsMessage, final String topicArn, final Map<String, String> attributes) {
        final String message = Utils.constructJson(snsMessage);
        final PublishRequest.Builder publishRequest = PublishRequest.builder()
                .message(message)
                .topicArn(topicArn);
        final Map<String, MessageAttributeValue> messageAttributesMap = new HashMap<>();

        if (!StringUtils.isEmpty(snsMessage.getTransactionId())) {
            messageAttributesMap.put(MessageConstants.TRANSACTION_ID, MessageAttributeValue.builder().dataType(STRING_DATA_TYPE).stringValue(snsMessage.getTransactionId()).build());
            messageAttributesMap.put(MessageConstants.MESSAGE_TYPE, MessageAttributeValue.builder().dataType(STRING_DATA_TYPE).stringValue(snsMessage.getMessageType()).build());
        }

        if (!CollectionUtils.isEmpty(attributes)) {
            messageAttributesMap.putAll(attributes.entrySet().stream()
                    .map(entry -> Tuple2.of(entry.getKey(),
                            MessageAttributeValue.builder()
                                    .stringValue(entry.getValue())
                                    .dataType(STRING_DATA_TYPE)))
                    .collect(Collectors.toMap(Tuple2::_1, tuple -> tuple._2().build())));
        }

        if (!CollectionUtils.isEmpty(messageAttributesMap)) {
            publishRequest.messageAttributes(messageAttributesMap);
        }

        return snsAsyncClient.publish(publishRequest.build());
    }
}
