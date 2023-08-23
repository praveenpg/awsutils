package org.awsutils.sqs.autoconfigure;

import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;



@ConfigurationProperties(prefix = "spring.pearson.aws.sqs.listener")
@Slf4j
public class SqsMessageListenerProperties {
    private int threadPoolSize;
    private String queueName;
    private String rateLimiterName;
    private String maximumNumberOfMessagesKey;
    private String messageHandlerRateLimiterName;
    private String statusProperty;
    private int maxMessageProcessorQueueSize = 500;
    private String numberOfListenersProperty;
    private String queueUrl;


    @NotNull(message = "spring.pearson.aws.sqs.listener.{name}.scheduleRunIntervalKey")
    private String scheduleRunIntervalKey;

    private String listenerName;
    private Integer waitTimeInSeconds;

    public SqsMessageListenerProperties() {
        log.info("Constructing SqsMessageListenerProperties");
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(final int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(final String queueName) {
        this.queueName = queueName;
    }

    public String getRateLimiterName() {
        return rateLimiterName;
    }

    public void setRateLimiterName(final String rateLimiterName) {
        this.rateLimiterName = rateLimiterName;
    }

    public String getMaximumNumberOfMessagesKey() {
        return maximumNumberOfMessagesKey;
    }

    public String getListenerName() {
        return listenerName;
    }

    public void setListenerName(final String listenerName) {
        this.listenerName = listenerName;
    }

    public void setMaximumNumberOfMessagesKey(final String maximumNumberOfMessagesKey) {
        this.maximumNumberOfMessagesKey = maximumNumberOfMessagesKey;
    }

    public String getScheduleRunIntervalKey() {
        return scheduleRunIntervalKey;
    }

    public void setScheduleRunIntervalKey(final String scheduleRunIntervalKey) {
        this.scheduleRunIntervalKey = scheduleRunIntervalKey;
    }

    public String getMessageHandlerRateLimiterName() {
        return messageHandlerRateLimiterName;
    }

    public void setMessageHandlerRateLimiterName(final String messageHandlerRateLimiterName) {
        this.messageHandlerRateLimiterName = messageHandlerRateLimiterName;
    }

    public String getStatusProperty() {
        return statusProperty;
    }

    public void setStatusProperty(final String statusProperty) {
        this.statusProperty = statusProperty;
    }

    public int getMaxMessageProcessorQueueSize() {
        return maxMessageProcessorQueueSize;
    }

    public void setMaxMessageProcessorQueueSize(final int maxMessageProcessorQueueSize) {
        this.maxMessageProcessorQueueSize = maxMessageProcessorQueueSize;
    }

    public Integer getWaitTimeInSeconds() {
        return waitTimeInSeconds;
    }

    public void setWaitTimeInSeconds(final Integer waitTimeInSeconds) {
        this.waitTimeInSeconds = waitTimeInSeconds;
    }

    public String getNumberOfListenersProperty() {
        return numberOfListenersProperty;
    }

    public void setNumberOfListenersProperty(final String numberOfListenersProperty) {
        this.numberOfListenersProperty = numberOfListenersProperty;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public void setQueueUrl(final String queueUrl) {
        this.queueUrl = queueUrl;
    }

    @Override
    public String toString() {
        return "SqsMessageListenerProperties{" +
                "threadPoolSize=" + threadPoolSize +
                ", queueName='" + queueName + '\'' +
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
