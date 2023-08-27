package org.awsutils.common.config;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import static org.awsutils.common.config.ConfigConstants.CONFIG_PREFIX;

@Getter
@Setter
@NoArgsConstructor
@ConfigurationProperties(prefix = CONFIG_PREFIX)
public class AwsEnvironmentProperties {
    private String region;
    private String awsAccessKey;
    private String awsAccessKeySecret;
    private int maxConcurrency = 100;
    private boolean localAwsMode = false;
    private String localAwsEndpoint;
    private int maxPendingConnectionAcquires = 10000;
}
