package org.awsutils.sqs.autoconfigure;

import org.awsutils.sqs.listener.SqsMessageListener;
import org.springframework.scheduling.annotation.SchedulingConfigurer;

import java.util.function.Function;

public interface SqsListenerScheduleConfig extends SchedulingConfigurer {
    void addListener(final SqsMessageListener sqsMessageListener, final String maxMessageKey, final String intervalKey, final Function<String, Integer> func1);
}
