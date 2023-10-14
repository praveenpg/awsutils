package org.awsutils.dynamodb.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EntityValidationInitializer {
    @Value("${org.awsutils.dynamodb.entities.basePackage:org.awsutils}")
    private String dtoBasePackage;

    @Bean
    EntityValidationConfig entityValidationConfig() {
        return new EntityValidationConfig(dtoBasePackage);
    }
}
