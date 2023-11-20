package org.awsutils.sqs.client;


import org.awsutils.sqs.message.SnsMessage;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class SnsServiceImpl extends AbstractSnsService<CompletableFuture<PublishResponse>> implements AsyncSnsService {
    private final SnsAsyncClient snsAsyncClient;

    public SnsServiceImpl(final SnsAsyncClient snsAsyncClient) {
        this.snsAsyncClient = snsAsyncClient;
    }

    @Override
    public <T> CompletableFuture<PublishResponse> publishMessage(final SnsMessage<T> snsMessage, final String topicArn, final Map<String, String> attributes) {
        return super.publishMessage(snsMessage, topicArn, attributes, snsAsyncClient::publish);
    }
}
