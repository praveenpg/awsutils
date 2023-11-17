package org.awsutils.sqs.client;

import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.util.concurrent.CompletableFuture;

public sealed interface AsyncSqsMessageClient extends SqsMessageClient<CompletableFuture<SendMessageResponse>,
        CompletableFuture<SendMessageBatchResponse>,
        CompletableFuture<DeleteMessageResponse>,
        CompletableFuture<ChangeMessageVisibilityResponse>> permits AsyncSqsMessageClientImpl {
}
