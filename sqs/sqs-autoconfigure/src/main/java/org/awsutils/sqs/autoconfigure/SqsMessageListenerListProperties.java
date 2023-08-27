package org.awsutils.sqs.autoconfigure;

import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Data
@ConfigurationProperties(prefix = "org.awsutils.aws.sqs")
public class SqsMessageListenerListProperties {
    private Map<String, SqsMessageListenerProperties> listener;
    private String handlerBasePackage = "org.awsutils";
}
