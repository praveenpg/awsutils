package org.awsutils.sqs.client;


import org.awsutils.sqs.message.SnsMessage;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.util.Map;

public final class SyncSnsServiceImpl extends AbstractSnsService<PublishResponse> implements SyncSnsService {
    private final SnsClient snsClient;

    public SyncSnsServiceImpl(final SnsClient snsClient) {
        this.snsClient = snsClient;
    }

    @Override
    public <T> PublishResponse publishMessage(final SnsMessage<T> snsMessage, final String topicArn, final Map<String, String> attributes) {
        return super.publishMessage(snsMessage, topicArn, attributes, snsClient::publish);
    }
}
