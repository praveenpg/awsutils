package org.awsutils.dynamodb.repositories;

import org.apache.commons.lang3.StringUtils;
import org.awsutils.common.config.AwsEnvironmentProperties;
import org.awsutils.dynamodb.config.DynamoDbProperties;
import org.awsutils.dynamodb.config.EntityValidationConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

@SuppressWarnings("rawtypes")
@Configuration
@ConditionalOnClass({DynamoDbRepository.class})
@EnableConfigurationProperties({DynamoDbProperties.class, AwsEnvironmentProperties.class})
@Import(DataMapperConfig.class)
public class DynamoDbAutoConfiguration {
    @Bean(name = "dynamoDbAsyncClientBuilder")
    @ConditionalOnBean(SdkAsyncHttpClient.class)
    public DynamoDbAsyncClientBuilder dynamoDbAsyncClientBuilder(final SdkAsyncHttpClient selectedSdkAsyncHttpClient,
                                                                 final AwsEnvironmentProperties awsEnvironmentProperties) throws URISyntaxException {

        final var builder = DynamoDbAsyncClient
                .builder()
                .region(Region.of(awsEnvironmentProperties.getRegion()))
                .httpClient(selectedSdkAsyncHttpClient);

        if(awsEnvironmentProperties.isLocalAwsMode() && !StringUtils.isEmpty(awsEnvironmentProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(awsEnvironmentProperties.getLocalAwsEndpoint()));
        }

        return builder;
    }

    @Bean(name = "dynamoDbClientBuilder")
    @ConditionalOnBean(SdkHttpClient.class)
    public DynamoDbClientBuilder dynamoDbClientBuilder(final SdkHttpClient selectedSdkHttpClient,
                                                       final AwsEnvironmentProperties awsEnvironmentProperties) throws URISyntaxException {

        final var builder = DynamoDbClient
                .builder()
                .region(Region.of(awsEnvironmentProperties.getRegion()))
                .httpClient(selectedSdkHttpClient);

        if(awsEnvironmentProperties.isLocalAwsMode() && !StringUtils.isEmpty(awsEnvironmentProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(awsEnvironmentProperties.getLocalAwsEndpoint()));
        }

        return builder;
    }

    @Bean(name = "dynamoDbAsyncClientBuilder")
    @ConditionalOnMissingBean(SdkAsyncHttpClient.class)
    public DynamoDbAsyncClientBuilder dynamoDbAsyncClientBuilder2(final AwsEnvironmentProperties awsEnvironmentProperties) throws URISyntaxException {

        final var builder = DynamoDbAsyncClient
                .builder()
                .region(Region.of(awsEnvironmentProperties.getRegion()));

        if(awsEnvironmentProperties.isLocalAwsMode() && !StringUtils.isEmpty(awsEnvironmentProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(awsEnvironmentProperties.getLocalAwsEndpoint()));
        }

        return builder;
    }

    @Bean(name = "dynamoDbClientBuilder")
    @ConditionalOnMissingBean(SdkHttpClient.class)
    public DynamoDbClientBuilder dynamoDbClientBuilder2(final AwsEnvironmentProperties awsEnvironmentProperties) throws URISyntaxException {

        final var builder = DynamoDbClient
                .builder()
                .region(Region.of(awsEnvironmentProperties.getRegion()));

        if(awsEnvironmentProperties.isLocalAwsMode() && !StringUtils.isEmpty(awsEnvironmentProperties.getLocalAwsEndpoint())) {
            return builder.endpointOverride(new URI(awsEnvironmentProperties.getLocalAwsEndpoint()));
        }

        return builder;
    }

    @Bean
    @ConditionalOnBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "org.awsutils.aws", value = {"region"})
    public DynamoDbAsyncClient amazonDynamoDB(final AwsCredentialsProvider staticCredentialsProvider,
                                              final DynamoDbAsyncClientBuilder dynamoDbAsyncClientBuilder) {

        return dynamoDbAsyncClientBuilder
                .credentialsProvider(staticCredentialsProvider)
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "org.awsutils.aws", value = {"region"})
    public DynamoDbAsyncClient amazonDynamoDBEnv(final DynamoDbAsyncClientBuilder dynamoDbAsyncClientBuilder) {
        return dynamoDbAsyncClientBuilder.build();
    }

    @Bean
    @ConditionalOnBean(DynamoDbAsyncClient.class)
    public DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient(final DynamoDbAsyncClient dynamoDbAsyncClient) {
        return DynamoDbEnhancedAsyncClient.builder().dynamoDbClient(dynamoDbAsyncClient).build();
    }

    @Bean
    @ConditionalOnBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "org.awsutils.aws", value = {"region"})
    public DynamoDbClient amazonDynamoSyncDB(final AwsCredentialsProvider staticCredentialsProvider,
                                             final DynamoDbClientBuilder dynamoDbAsyncClientBuilder) {

        return dynamoDbAsyncClientBuilder
                .credentialsProvider(staticCredentialsProvider)
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "staticCredentialsProvider")
    @ConditionalOnProperty(prefix = "org.awsutils.aws", value = {"region"})
    public DynamoDbClient amazonDynamoSyncDBEnv(final DynamoDbClientBuilder dynamoDbAsyncClientBuilder) {
        return dynamoDbAsyncClientBuilder.build();
    }

    @Bean
    @ConditionalOnBean(DynamoDbClient.class)
    public DynamoDbEnhancedClient dynamoDbEnhancedClient(final DynamoDbClient dynamoDbClient) {
        return DynamoDbEnhancedClient.builder().dynamoDbClient(dynamoDbClient).build();
    }

    @Bean(name = "entityValidationConfigMain")
    @ConditionalOnProperty(prefix = "org.awsutils.aws.ddb", value = "entity-base-package")
    public EntityValidationConfig entityValidationConfigMain(final DynamoDbProperties dynamoDbProperties) {
        return new EntityValidationConfig(dynamoDbProperties.getEntityBasePackage());
    }

    @Bean(name = "dataMapperConfigCleanUpMain")
    @ConditionalOnProperty(prefix = "org.awsutils.aws.ddb", value = "entity-base-package")
    public DataMapperConfigCleanUp dataMapperConfigCleanUpMain(final DynamoDbProperties dynamoDbProperties, final Map<Class, DataMapper> dataMapperMap, final Environment environment) {
        return new DataMapperConfigCleanUp(dynamoDbProperties.getEntityBasePackage(), dataMapperMap, environment);
    }
}
