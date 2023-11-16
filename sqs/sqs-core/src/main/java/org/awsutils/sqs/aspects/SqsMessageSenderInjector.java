package org.awsutils.sqs.aspects;

import org.awsutils.sqs.client.SyncSqsMessageClient;

public interface SqsMessageSenderInjector {
    SyncSqsMessageClient sqsMessageClient();
}
