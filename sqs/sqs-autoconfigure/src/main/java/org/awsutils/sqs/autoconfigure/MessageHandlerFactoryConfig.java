package org.awsutils.sqs.autoconfigure;

import io.vavr.Tuple2;
import org.awsutils.sqs.client.SnsService;
import org.awsutils.sqs.client.SnsServiceImpl;
import org.awsutils.sqs.client.SqsMessageClient;
import org.awsutils.sqs.client.SqsMessageClientImpl;
import org.awsutils.sqs.handler.MessageHandlerFactory;
import org.awsutils.sqs.handler.MessageHandlerFactoryImpl;
import org.awsutils.sqs.handler.impl.AbstractSqsMessageHandler;
import org.awsutils.common.util.ApplicationContextUtils;
import org.awsutils.common.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

@SuppressWarnings({"SpringFacetCodeInspection", "FieldCanBeLocal", "unused", "unchecked", "rawtypes"})
@Configuration
@Slf4j
public class MessageHandlerFactoryConfig {
    private final Map<String, Tuple2<Constructor<AbstractSqsMessageHandler>, Method>> handlerMapping;
    private final SqsAsyncClient sqsAsyncClient;
    private final SnsAsyncClient snsAsyncClient;
    private final ApplicationContext applicationContext;
    private final Environment environment;
    private final Map<String, Method> methodHandlerMapping;


    public MessageHandlerFactoryConfig(final SqsAsyncClient sqsAsyncClient,
                                       final SnsAsyncClient snsAsyncClient, final ApplicationContext applicationContext,
                                       final Environment environment) throws InvocationTargetException, IllegalAccessException {

        final Method method = Utils.getMethod(ApplicationContextUtils.class, "init", ApplicationContext.class, Environment.class);
        method.invoke(null, applicationContext, environment);

        this.handlerMapping = (Map<String, Tuple2<Constructor<AbstractSqsMessageHandler>, Method>>) applicationContext.getBean("sqsMessageHandlerMapping");
        this.methodHandlerMapping = (Map<String, Method>) applicationContext.getBean("sqsMethodLevelMessageHandlerMapping");
        this.sqsAsyncClient = sqsAsyncClient;
        this.snsAsyncClient = snsAsyncClient;
        this.applicationContext = applicationContext;
        this.environment = environment;
    }

    @Bean
    public MessageHandlerFactory messageHandlerFactory() {
        return new MessageHandlerFactoryImpl(handlerMapping, methodHandlerMapping, applicationContext);
    }

    @Bean
    public SqsMessageClient sqsMessageClient() {
        return new SqsMessageClientImpl(sqsAsyncClient);
    }

    @Bean
    public SnsService snsService() {
        return new SnsServiceImpl(snsAsyncClient);
    }
}
