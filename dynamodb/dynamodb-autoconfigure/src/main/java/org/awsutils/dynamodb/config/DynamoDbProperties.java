package org.awsutils.dynamodb.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import static org.awsutils.dynamodb.config.DdbConfigConstants.CONFIG_PREFIX;


@ConfigurationProperties(prefix = CONFIG_PREFIX)
@Data
public class DynamoDbProperties {
    private String entityBasePackage;
    private String repositoryBasePackage;
}
