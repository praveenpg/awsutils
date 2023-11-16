package org.awsutils.sqs.aspects;

import org.awsutils.common.util.ApplicationContextUtils;
import org.awsutils.sqs.client.SyncSqsMessageClient;

public class SqsMessageSenderInjectorImpl implements SqsMessageSenderInjector {
    @Override
    public SyncSqsMessageClient sqsMessageClient() {
        return ApplicationContextUtils.getInstance().getBean("syncSqsMessageClient");
    }
}
