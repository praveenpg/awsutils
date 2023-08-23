package org.awsutils.sqs.autoconfigure;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ConfigurationProperties(prefix = "spring.pearson.aws")
public class AwsEnvironmentProperties {
    private String region;
    private String awsAccessKey;
    private String awsAccessKeySecret;
    private int maxConcurrency = 100;
    private boolean localAwsMode = false;
    private String localAwsEndpoint;
    private int maxPendingConnectionAcquires = 10000;
}
