package org.awsutils.common.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

import static org.awsutils.common.config.ConfigConstants.CONFIG_PREFIX;

@Data
@ConfigurationProperties(prefix = CONFIG_PREFIX)
public class AwsEnvironmentProperties {
    private String region;
    private String awsAccessKey;
    private String awsAccessKeySecret;
    private int maxConcurrency = 100;
    private boolean localAwsMode = false;
    private String localAwsEndpoint;
    private Duration connectionTimeout = Duration.ofSeconds(5);
    private Duration connectionMaxIdleTime = Duration.ofSeconds(5);
}
