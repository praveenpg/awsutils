package org.awsutils.sqs.autoconfigure;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.awsutils.common.config.AwsEnvironmentProperties;
import org.awsutils.common.util.LimitedQueue;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.SnsAsyncClientBuilder;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.awsutils.common.config.ConfigConstants.CONFIG_PREFIX;

@Configuration
@Slf4j
public class SqsConfig {
    @Bean("snsAsyncClientBuilder")
    @ConditionalOnBean(SdkAsyncHttpClient.class)
    public SnsAsyncClientBuilder snsAsyncClientBuilder_1(final SdkAsyncHttpClient selectedSdkAsyncHttpClient, final AwsEnvironmentProperties sqsProperties) throws URISyntaxException {
        final var builder = SnsAsyncClient.builder().region(Region.of(sqsProperties.getRegion()))
                .httpClient(selectedSdkAsyncHttpClient);

        if(sqsProperties.isLocalAwsMode() && !StringUtils.isEmpty(sqsProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(sqsProperties.getLocalAwsEndpoint()));
        }

        return builder;
    }

    @Bean("snsAsyncClientBuilder")
    @ConditionalOnMissingBean(SdkAsyncHttpClient.class)
    public SnsAsyncClientBuilder snsAsyncClientBuilder_2(final AwsEnvironmentProperties sqsProperties) throws URISyntaxException {
        final var builder = SnsAsyncClient.builder().region(Region.of(sqsProperties.getRegion()));

        if(sqsProperties.isLocalAwsMode() && !StringUtils.isEmpty(sqsProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(sqsProperties.getLocalAwsEndpoint()));
        }

        return builder;
    }

    //----------------------------------------------------------------------------------------------------

    @Bean("snsSyncClientBuilder")
    @ConditionalOnBean(SdkAsyncHttpClient.class)
    public SnsClientBuilder snsAsyncClientBuilder_1(final SdkHttpClient selectedSdkAsyncHttpClient, final AwsEnvironmentProperties sqsProperties) throws URISyntaxException {
        final var builder = SnsClient.builder().region(Region.of(sqsProperties.getRegion()))
                .httpClient(selectedSdkAsyncHttpClient);

        if(sqsProperties.isLocalAwsMode() && !StringUtils.isEmpty(sqsProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(sqsProperties.getLocalAwsEndpoint()));
        }

        return builder;
    }

    @Bean("snsSyncClientBuilder")
    @ConditionalOnMissingBean(SdkAsyncHttpClient.class)
    public SnsClientBuilder snsSyncClientBuilder_2(final AwsEnvironmentProperties sqsProperties) throws URISyntaxException {
        final var builder = SnsClient.builder().region(Region.of(sqsProperties.getRegion()));

        if(sqsProperties.isLocalAwsMode() && !StringUtils.isEmpty(sqsProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(sqsProperties.getLocalAwsEndpoint()));
        }

        return builder;
    }

    //----------------------------------------------------------------------------------------------------

    @Bean("sqsAsyncClientBuilder")
    @ConditionalOnBean(SdkAsyncHttpClient.class)
    public SqsAsyncClientBuilder sqsAsyncClientBuilder_1(final SdkAsyncHttpClient selectedSdkAsyncHttpClient, final AwsEnvironmentProperties sqsProperties) throws URISyntaxException {
        final var builder = SqsAsyncClient
                .builder()
                .region(Region.of(sqsProperties.getRegion()))
                .httpClient(selectedSdkAsyncHttpClient);

        if(sqsProperties.isLocalAwsMode() && !StringUtils.isEmpty(sqsProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(sqsProperties.getLocalAwsEndpoint()));
        }

        return builder;
    }

    @Bean("sqsSyncClientBuilder")
    @ConditionalOnBean(SdkHttpClient.class)
    public SqsClientBuilder sqsSyncClientBuilder_1(final SdkHttpClient sdkHttpClient, final AwsEnvironmentProperties sqsProperties) throws URISyntaxException {
        final var builder = SqsClient
                .builder()
                .region(Region.of(sqsProperties.getRegion()))
                .httpClient(sdkHttpClient);

        if(sqsProperties.isLocalAwsMode() && !StringUtils.isEmpty(sqsProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(sqsProperties.getLocalAwsEndpoint()));
        }

        return builder;
    }

    @Bean("sqsAsyncClientBuilder")
    @ConditionalOnMissingBean(SdkAsyncHttpClient.class)
    public SqsAsyncClientBuilder sqsAsyncClientBuilder_2(final AwsEnvironmentProperties sqsProperties) throws URISyntaxException {
        final var builder = SqsAsyncClient.builder().region(Region.of(sqsProperties.getRegion()));

        if(sqsProperties.isLocalAwsMode() && !StringUtils.isEmpty(sqsProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(sqsProperties.getLocalAwsEndpoint()));
        }

        return builder;
    }

    @Bean("sqsSyncClientBuilder")
    @ConditionalOnMissingBean(SdkHttpClient.class)
    public SqsClientBuilder sqsClientBuilder_2(final AwsEnvironmentProperties sqsProperties) throws URISyntaxException {
        final var builder = SqsClient.builder().region(Region.of(sqsProperties.getRegion()));

        if(sqsProperties.isLocalAwsMode() && !StringUtils.isEmpty(sqsProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(sqsProperties.getLocalAwsEndpoint()));
        }

        return builder;
    }

    @Bean
    @ConditionalOnBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = CONFIG_PREFIX, value = {"region"})
    public SnsAsyncClient snsAsyncClient(final SnsAsyncClientBuilder snsAsyncClientBuilder,
                                         final AwsCredentialsProvider staticCredentialsProvider) {

        return snsAsyncClientBuilder.credentialsProvider(staticCredentialsProvider).build();
    }

    @Bean
    @ConditionalOnBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = CONFIG_PREFIX, value = {"region"})
    public SnsClient snsSyncClient(final SnsClientBuilder snsClientBuilder,
                                    final AwsCredentialsProvider staticCredentialsProvider) {

        return snsClientBuilder.credentialsProvider(staticCredentialsProvider).build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = CONFIG_PREFIX, value = {"region"})
    public SnsAsyncClient snsAsyncClientEnv(final SnsAsyncClientBuilder snsAsyncClientBuilder) {

        return snsAsyncClientBuilder.build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = CONFIG_PREFIX, value = {"region"})
    public SnsClient snsSyncClientEnv(final SnsClientBuilder snsAsyncClientBuilder) {

        return snsAsyncClientBuilder.build();
    }

    @Bean
    @ConditionalOnBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = CONFIG_PREFIX, value = {"region"})
    public SqsAsyncClient sqsAsyncClient(final AwsCredentialsProvider staticCredentialsProvider,
                                         final SqsAsyncClientBuilder sqsAsyncClientBuilder) {

        return sqsAsyncClientBuilder.credentialsProvider(staticCredentialsProvider).build();
    }

    @Bean
    @ConditionalOnBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = CONFIG_PREFIX, value = {"region"})
    public SqsClient sqsSyncClient(final AwsCredentialsProvider staticCredentialsProvider,
                                         final SqsClientBuilder sqsClientBuilder) {

        return sqsClientBuilder.credentialsProvider(staticCredentialsProvider).build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = CONFIG_PREFIX, value = {"region"})
    public SqsAsyncClient sqsAsyncClientEnv(final SqsAsyncClientBuilder sqsAsyncClientBuilder) {

        return sqsAsyncClientBuilder.build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = CONFIG_PREFIX, value = {"region"})
    public SqsClient sqsSyncClientEnv(final SqsClientBuilder sqsClientBuilder) {

        return sqsClientBuilder.build();
    }

    @Bean(name = "messagePollingIntervalPropertyPropertyFunc")
    @ConditionalOnMissingBean(name = "messagePollingIntervalPropertyFF4jFunc")
    public SqsPropertyFunc1<String, Integer> messagePollingIntervalPropertyPropertyFunc(final Environment environment) {
        return propertyName -> {
            try {
                return Integer.parseInt(Objects.requireNonNull(environment.getProperty(propertyName)));
            } catch (RuntimeException e) {
                log.error(MessageFormat.format("Error while getting property: {0}. Please make sure the property is present in application.yaml/application.properties/CCS", propertyName));
                throw e;
            }
        };
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnProperty(prefix = "org.awsutils.aws.sqs.common", name = "threadPoolSize")
    public CommonExecutorService commonExecutorService(final SqsCommonProperties sqsCommonProperties) {
        return new CommonExecutorService(getThreadPoolExecutor(sqsCommonProperties.getThreadPoolCoreSize(), sqsCommonProperties.getThreadPoolSize(), sqsCommonProperties.getMaxThreadPoolQueueSize()));
    }

    @Bean
    @ConditionalOnMissingBean(TaskScheduler.class)
    public TaskScheduler taskScheduler(final SqsCommonProperties sqsCommonProperties) {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();

        scheduler.setPoolSize(sqsCommonProperties.getTaskExecutorThreadPoolSize());

        return scheduler;
    }

    private ThreadPoolExecutor getThreadPoolExecutor(final int coreSize, final int maxPoolSize, final int maxPoolQueueSize) {
        return getThreadPoolExecutor(coreSize, maxPoolSize, maxPoolQueueSize, 0L, TimeUnit.MILLISECONDS);
    }

    private ThreadPoolExecutor getThreadPoolExecutor(final int coreSize, final int maxPoolSize, final int maxPoolQueueSize, final long keepAliveTime, final TimeUnit keepAliveTimeUnit) {
        final LimitedQueue<Runnable> runnables = new LimitedQueue<>(maxPoolQueueSize);
        final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(coreSize, maxPoolSize,
                keepAliveTime, keepAliveTimeUnit,
                runnables, new LimitedQueue.LimitedQueueRejectedExecutionPolicy());

        runnables.setThreadPoolExecutor(threadPoolExecutor);

        return threadPoolExecutor;
    }

    public record CommonExecutorService(ExecutorService executorService) {

        void shutdown() {
                executorService.shutdown();
            }
        }
    public interface SqsPropertyFunc1<T, R> extends Function<T, R> {}
}
