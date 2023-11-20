package org.awsutils.sqs.client;

import org.awsutils.sqs.message.SnsMessage;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface SnsService<A> {

    default <T> A publishMessage(SnsMessage<T> snsMessage, String topicArn) {
        return publishMessage(snsMessage, topicArn, Collections.emptyMap());
    }

    <T> A publishMessage(SnsMessage<T> snsMessage, String topicArn, Map<String, String> attributes);
}
