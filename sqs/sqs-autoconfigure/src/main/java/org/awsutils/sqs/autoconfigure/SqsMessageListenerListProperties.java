package org.awsutils.sqs.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties(prefix = "spring.pearson.aws.sqs")
public class SqsMessageListenerListProperties {
    private Map<String, SqsMessageListenerProperties> listener;
    private String handlerBasePackage = "org.awsutils";

    public Map<String, SqsMessageListenerProperties> getListener() {
        return listener;
    }

    public void setListener(final Map<String, SqsMessageListenerProperties> listener) {
        this.listener = listener;
    }

    public String getHandlerBasePackage() {
        return handlerBasePackage;
    }

    public void setHandlerBasePackage(String handlerBasePackage) {
        this.handlerBasePackage = handlerBasePackage;
    }

    @Override
    public String toString() {
        return "SqsMessageListenerListProperties{" +
                "listener=" + listener +
                ", handlerBasePackage='" + handlerBasePackage + '\'' +
                '}';
    }
}
