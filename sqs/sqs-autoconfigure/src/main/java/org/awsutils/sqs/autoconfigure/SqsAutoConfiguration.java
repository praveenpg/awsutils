package org.awsutils.sqs.autoconfigure;

import org.awsutils.common.config.AwsEnvironmentProperties;
import org.awsutils.sqs.message.SqsMessage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ConditionalOnClass({SqsMessage.class})
@EnableConfigurationProperties({SqsCommonProperties.class, SqsMessageListenerProperties.class, SqsMessageListenerListProperties.class,  AwsEnvironmentProperties.class})
@Import({SqsConfig.class, TaskRegistrarConfig.class, SqsMessageHandlerConfig.class, MessageHandlerFactoryConfig.class, SqsMessageListenerInitializer.class})
public class SqsAutoConfiguration {
}
