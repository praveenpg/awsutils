package org.awsutils.sqs.aspects;

import org.awsutils.sqs.client.SqsMessageClient;
import org.awsutils.sqs.util.ApplicationContextUtils;

public class SqsMessageSenderInjectorImpl implements SqsMessageSenderInjector {
    @Override
    public SqsMessageClient sqsMessageClient() {
        return ApplicationContextUtils.getInstance().getBean("sqsMessageClient");
    }
}
