package org.awsutils.sqs.autoconfigure;

import org.awsutils.sqs.util.LimitedQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Configuration
@Slf4j
public class SqsConfig {
    @Bean("staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "spring.pearson.aws", value = {"awsAccessKeySecret", "awsAccessKey"})
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    public AwsCredentialsProvider staticCredentialsProvider(final AwsEnvironmentProperties sqsProperties) {
        return StaticCredentialsProvider
                .create(AwsBasicCredentials.create(sqsProperties.getAwsAccessKey(), sqsProperties.getAwsAccessKeySecret()));
    }

    @Bean("staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "spring.pearson.aws", value = {"aws-access-key", "aws-access-key-secret"})
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    public AwsCredentialsProvider staticCredentialsProvider2(final AwsEnvironmentProperties sqsProperties) {
        return StaticCredentialsProvider
                .create(AwsBasicCredentials.create(sqsProperties.getAwsAccessKey(), sqsProperties.getAwsAccessKeySecret()));
    }

    @Bean("staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "spring.pearson.aws", value = {"awsAccessKeySecret", "aws-access-key-secret"})
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    public AwsCredentialsProvider staticCredentialsProvider3(final AwsEnvironmentProperties sqsProperties) {
        return StaticCredentialsProvider
                .create(AwsBasicCredentials.create(sqsProperties.getAwsAccessKey(), sqsProperties.getAwsAccessKeySecret()));
    }

    @Bean("staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "spring.pearson.aws", value = {"aws-access-key", "awsAccessKeySecret"})
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    public AwsCredentialsProvider staticCredentialsProvider4(final AwsEnvironmentProperties sqsProperties) {
        return StaticCredentialsProvider
                .create(AwsBasicCredentials.create(sqsProperties.getAwsAccessKey(), sqsProperties.getAwsAccessKeySecret()));
    }

    @Bean
    @ConditionalOnBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "spring.pearson.aws", value = {"region"})
    public SnsAsyncClient snsAsyncClient(final AwsCredentialsProvider staticCredentialsProvider,
//                                         final SdkAsyncHttpClient selectedSdkAsyncHttpClient,
                                         final AwsEnvironmentProperties sqsProperties) throws URISyntaxException {

        final var builder = SnsAsyncClient
                .builder()
                //.httpClient(selectedSdkAsyncHttpClient)
                .region(Region.of(sqsProperties.getRegion())).credentialsProvider(staticCredentialsProvider);

        if(sqsProperties.isLocalAwsMode() && !StringUtils.isEmpty(sqsProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(sqsProperties.getLocalAwsEndpoint())).build();
        } else {
            return builder.build();
        }
    }

    @Bean
    @ConditionalOnBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "spring.pearson.aws", value = {"region"})
    public SqsAsyncClient sqsAsyncClient(final AwsCredentialsProvider staticCredentialsProvider,
//                                         final SdkAsyncHttpClient selectedSdkAsyncHttpClient,
                                         final AwsEnvironmentProperties sqsProperties) throws URISyntaxException {

        final var builder = SqsAsyncClient
                .builder()
                //.httpClient(selectedSdkAsyncHttpClient)
                .region(Region.of(sqsProperties.getRegion())).credentialsProvider(staticCredentialsProvider);

        if(sqsProperties.isLocalAwsMode() && !StringUtils.isEmpty(sqsProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(sqsProperties.getLocalAwsEndpoint())).build();
        } else {
            return builder.build();
        }
    }

    @Bean
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "spring.pearson.aws", value = {"region"})
    public SqsAsyncClient sqsAsyncClientEnv(final AwsEnvironmentProperties sqsProperties
//                                            ,final SdkAsyncHttpClient selectedSdkAsyncHttpClient
    ) throws URISyntaxException {

        final var builder = SqsAsyncClient
                .builder()
                //.httpClient(selectedSdkAsyncHttpClient)
                .region(Region.of(sqsProperties.getRegion()));

        if(sqsProperties.isLocalAwsMode() && !StringUtils.isEmpty(sqsProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(sqsProperties.getLocalAwsEndpoint())).build();
        } else {
            return builder.build();
        }
    }

    @Bean
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "spring.pearson.aws", value = {"region"})
    public SnsAsyncClient snsAsyncClientEnv(final AwsEnvironmentProperties sqsProperties
//                                            ,final SdkAsyncHttpClient selectedSdkAsyncHttpClient
    ) throws URISyntaxException {

        final var builder = SnsAsyncClient
                .builder()
                //.httpClient(selectedSdkAsyncHttpClient)
                .region(Region.of(sqsProperties.getRegion()));

        if(sqsProperties.isLocalAwsMode() && !StringUtils.isEmpty(sqsProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(sqsProperties.getLocalAwsEndpoint())).build();
        } else {
            return builder.build();
        }
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
    @ConditionalOnProperty(prefix = "spring.pearson.aws.sqs.common", name = "threadPoolSize")
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
