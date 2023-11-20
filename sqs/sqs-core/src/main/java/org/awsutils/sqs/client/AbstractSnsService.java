package org.awsutils.sqs.client;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.awsutils.common.util.Utils;
import org.awsutils.sqs.message.SnsMessage;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

abstract sealed class AbstractSnsService<A> implements SnsService<A> permits SyncSnsServiceImpl, SnsServiceImpl {
    private static final String STRING_DATA_TYPE = "String";

    protected <T> A publishMessage(final SnsMessage<T> snsMessage,
                                   final String topicArn,
                                   final Map<String, String> attributes,
                                   final Function<PublishRequest, A> func) {

        final String message = Utils.constructJson(snsMessage);
        final PublishRequest.Builder publishRequest = PublishRequest.builder()
                .message(message)
                .topicArn(topicArn);
        final Map<String, MessageAttributeValue> messageAttributesMap = new HashMap<>();

        if (StringUtils.hasLength(snsMessage.getTransactionId())) {
            messageAttributesMap.put(MessageConstants.TRANSACTION_ID, MessageAttributeValue.builder().dataType(STRING_DATA_TYPE).stringValue(snsMessage.getTransactionId()).build());
            messageAttributesMap.put(MessageConstants.MESSAGE_TYPE, MessageAttributeValue.builder().dataType(STRING_DATA_TYPE).stringValue(snsMessage.getMessageType()).build());
        }

        if (!CollectionUtils.isEmpty(attributes)) {
            messageAttributesMap.putAll(attributes.entrySet().stream()
                    .map(entry -> Tuple.of(entry.getKey(),
                            MessageAttributeValue.builder()
                                    .stringValue(entry.getValue())
                                    .dataType(STRING_DATA_TYPE)))
                    .collect(Collectors.toMap(Tuple2::_1, tuple -> tuple._2().build())));
        }

        if (!CollectionUtils.isEmpty(messageAttributesMap)) {
            publishRequest.messageAttributes(messageAttributesMap);
        }

        return func.apply(publishRequest.build());
    }
}
