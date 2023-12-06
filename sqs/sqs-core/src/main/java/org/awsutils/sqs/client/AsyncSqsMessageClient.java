package org.awsutils.sqs.client;

import software.amazon.awssdk.services.sqs.model.*;

import java.util.concurrent.CompletableFuture;

public sealed interface AsyncSqsMessageClient extends SqsMessageClient<CompletableFuture<SendMessageResponse>,
        CompletableFuture<SendMessageBatchResponse>,
        CompletableFuture<DeleteMessageResponse>,
        CompletableFuture<ChangeMessageVisibilityResponse>, CompletableFuture<GetQueueUrlResponse>> permits AsyncSqsMessageClientImpl {

}
