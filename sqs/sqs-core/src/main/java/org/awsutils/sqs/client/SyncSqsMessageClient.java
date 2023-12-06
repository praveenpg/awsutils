package org.awsutils.sqs.client;

import software.amazon.awssdk.services.sqs.model.*;

public sealed interface SyncSqsMessageClient extends SqsMessageClient<SendMessageResponse, SendMessageBatchResponse,
        DeleteMessageResponse, ChangeMessageVisibilityResponse, GetQueueUrlResponse> permits SyncSqsMessageClientImpl {
}
