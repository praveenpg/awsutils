package org.awsutils.dynamodb.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import static org.awsutils.common.config.ConfigConstants.CONFIG_PREFIX;

@ConfigurationProperties(prefix = CONFIG_PREFIX)
public class DynamoDbProperties {
    private String entityBasePackage;
    private String repositoryBasePackage;

    public String getEntityBasePackage() {
        return entityBasePackage;
    }

    public void setEntityBasePackage(final String entityBasePackage) {
        this.entityBasePackage = entityBasePackage;
    }

    public String getRepositoryBasePackage() {
        return repositoryBasePackage;
    }

    public void setRepositoryBasePackage(final String repositoryBasePackage) {
        this.repositoryBasePackage = repositoryBasePackage;
    }
}
