package org.awsutils.sqs.aspects;

import org.awsutils.sqs.client.SqsMessageClient;

public interface SqsMessageSenderInjector {
    SqsMessageClient sqsMessageClient();
}
