package org.awsutils.sqs.message;

import java.util.Map;

public interface AwsMessage {
    String getMessageType();

    Map<String, ?> getMessage();

    String getTransactionId();
}
