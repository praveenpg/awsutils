package org.awsutils.sqs.autoconfigure;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;



@Getter
@ConfigurationProperties(prefix = "org.awsutils.aws.sqs.listener")
@Slf4j
public class SqsMessageListenerProperties {
    private int threadPoolSize;
    private String rateLimiterName;
    private String maximumNumberOfMessagesKey;
    private String messageHandlerRateLimiterName;
    private String statusProperty;
    private int maxMessageProcessorQueueSize = 500;
    private String numberOfListenersProperty;
    private String queueUrl;


    @NotNull(message = "org.awsutils.aws.sqs.listener.{name}.scheduleRunIntervalKey")
    private String scheduleRunIntervalKey;

    private String listenerName;
    private Integer waitTimeInSeconds;

    public SqsMessageListenerProperties() {
        log.info("Constructing SqsMessageListenerProperties");
    }

    public void setThreadPoolSize(final int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public void setRateLimiterName(final String rateLimiterName) {
        this.rateLimiterName = rateLimiterName;
    }

    public void setListenerName(final String listenerName) {
        this.listenerName = listenerName;
    }

    public void setMaximumNumberOfMessagesKey(final String maximumNumberOfMessagesKey) {
        this.maximumNumberOfMessagesKey = maximumNumberOfMessagesKey;
    }

    public void setScheduleRunIntervalKey(final String scheduleRunIntervalKey) {
        this.scheduleRunIntervalKey = scheduleRunIntervalKey;
    }

    public void setMessageHandlerRateLimiterName(final String messageHandlerRateLimiterName) {
        this.messageHandlerRateLimiterName = messageHandlerRateLimiterName;
    }

    public void setStatusProperty(final String statusProperty) {
        this.statusProperty = statusProperty;
    }

    public void setMaxMessageProcessorQueueSize(final int maxMessageProcessorQueueSize) {
        this.maxMessageProcessorQueueSize = maxMessageProcessorQueueSize;
    }

    public void setWaitTimeInSeconds(final Integer waitTimeInSeconds) {
        this.waitTimeInSeconds = waitTimeInSeconds;
    }

    public void setNumberOfListenersProperty(final String numberOfListenersProperty) {
        this.numberOfListenersProperty = numberOfListenersProperty;
    }

    public void setQueueUrl(final String queueUrl) {
        this.queueUrl = queueUrl;
    }

    @Override
    public String toString() {
        return "SqsMessageListenerProperties{" +
                "threadPoolSize=" + threadPoolSize +
                ", rateLimiterName='" + rateLimiterName + '\'' +
                ", maximumNumberOfMessagesKey='" + maximumNumberOfMessagesKey + '\'' +
                ", messageHandlerRateLimiterName='" + messageHandlerRateLimiterName + '\'' +
                ", statusProperty='" + statusProperty + '\'' +
                ", maxMessageProcessorQueueSize=" + maxMessageProcessorQueueSize +
                ", numberOfListeners=" + numberOfListenersProperty +
                ", scheduleRunIntervalKey='" + scheduleRunIntervalKey + '\'' +
                ", listenerName='" + listenerName + '\'' +
                ", waitTimeInSeconds=" + waitTimeInSeconds +
                '}';
    }
}
