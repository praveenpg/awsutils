package org.awsutils.sqs.client;

import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.util.concurrent.CompletableFuture;

public sealed interface AsyncSnsService extends SnsService<CompletableFuture<PublishResponse>> permits SnsServiceImpl {
}
