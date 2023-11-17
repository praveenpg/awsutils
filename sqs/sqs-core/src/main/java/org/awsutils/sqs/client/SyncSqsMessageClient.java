package org.awsutils.sqs.client;

import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public sealed interface SyncSqsMessageClient extends SqsMessageClient<SendMessageResponse, SendMessageBatchResponse,
        DeleteMessageResponse, ChangeMessageVisibilityResponse> permits SyncSqsMessageClientImpl {
}
