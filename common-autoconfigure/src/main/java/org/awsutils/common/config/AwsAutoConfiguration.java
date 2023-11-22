package org.awsutils.common.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.internal.DefaultConfiguration;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;

import java.util.Optional;

import static org.awsutils.common.config.ConfigConstants.CONFIG_PREFIX;

@Configuration
@EnableConfigurationProperties({AwsEnvironmentProperties.class})
@Slf4j
public class AwsAutoConfiguration {
    @Bean("staticCredentialsProvider")
    @ConditionalOnProperty(prefix = CONFIG_PREFIX, value = {"awsAccessKeySecret", "awsAccessKey"})
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    public AwsCredentialsProvider staticCredentialsProvider(final AwsEnvironmentProperties sqsProperties) {
        return StaticCredentialsProvider
                .create(AwsBasicCredentials.create(sqsProperties.getAwsAccessKey(), sqsProperties.getAwsAccessKeySecret()));
    }

    @Bean("staticCredentialsProvider")
    @ConditionalOnProperty(prefix = CONFIG_PREFIX, value = {"aws-access-key", "aws-access-key-secret"})
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    public AwsCredentialsProvider staticCredentialsProvider2(final AwsEnvironmentProperties awsEnvironmentProperties) {
        return StaticCredentialsProvider
                .create(AwsBasicCredentials.create(awsEnvironmentProperties.getAwsAccessKey(), awsEnvironmentProperties.getAwsAccessKeySecret()));
    }

    @Bean("staticCredentialsProvider")
    @ConditionalOnProperty(prefix = CONFIG_PREFIX, value = {"awsAccessKeySecret", "aws-access-key-secret"})
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    public AwsCredentialsProvider staticCredentialsProvider3(final AwsEnvironmentProperties awsEnvironmentProperties) {
        return StaticCredentialsProvider
                .create(AwsBasicCredentials.create(awsEnvironmentProperties.getAwsAccessKey(), awsEnvironmentProperties.getAwsAccessKeySecret()));
    }

    @Bean("staticCredentialsProvider")
    @ConditionalOnProperty(prefix = CONFIG_PREFIX, value = {"aws-access-key", "awsAccessKeySecret"})
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    public AwsCredentialsProvider staticCredentialsProvider4(final AwsEnvironmentProperties awsEnvironmentProperties) {
        return StaticCredentialsProvider
                .create(AwsBasicCredentials.create(awsEnvironmentProperties.getAwsAccessKey(), awsEnvironmentProperties.getAwsAccessKeySecret()));
    }

    @Bean
    @ConditionalOnMissingBean(SdkAsyncHttpClient.class)
    public SdkAsyncHttpClient sdkAsyncHttpClient(final AwsEnvironmentProperties awsEnvironmentProperties) {
        log.info("Constructing defaultSdkAsyncHttpClient");
        return AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(awsEnvironmentProperties.getMaxConcurrency())
                .connectionTimeout(awsEnvironmentProperties.getConnectionTimeout())
                .connectionMaxIdleTime(awsEnvironmentProperties.getConnectionMaxIdleTime())
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(SdkHttpClient.class)
    public SdkHttpClient sdkHttpClient(final AwsEnvironmentProperties awsEnvironmentProperties) {
        log.info("Constructing defaultSdkAsyncHttpClient");
        return ApacheHttpClient.builder()
                .connectionAcquisitionTimeout(awsEnvironmentProperties.getConnectionAcquisitionTimeout())
                .expectContinueEnabled(Optional.ofNullable(awsEnvironmentProperties.getExpectContinueEnabled())
                        .orElse(DefaultConfiguration.EXPECT_CONTINUE_ENABLED))
                .connectionTimeToLive(awsEnvironmentProperties.getConnectionTimeToLive())
                .maxConnections(awsEnvironmentProperties.getMaxConcurrency())
                .connectionTimeout(awsEnvironmentProperties.getConnectionTimeout())
                .connectionMaxIdleTime(awsEnvironmentProperties.getConnectionMaxIdleTime())
                .build();
    }
}
