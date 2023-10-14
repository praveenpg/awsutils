package org.awsutils.sqs.autoconfigure;


import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.awsutils.sqs.annotations.MessageHandler;
import org.awsutils.common.exceptions.UtilsException;
import org.awsutils.sqs.handler.impl.AbstractSqsMessageHandler;
import org.awsutils.sqs.message.SqsMessage;
import org.awsutils.common.ratelimiter.RateLimiter;
import org.awsutils.common.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.reflections.scanners.Scanners.MethodsAnnotated;
import static org.reflections.scanners.Scanners.TypesAnnotated;

@SuppressWarnings("SpringFacetCodeInspection")
@Configuration
@Slf4j
public class SqsMessageHandlerConfig {
    private final SqsMessageListenerListProperties sqsMessageListenerListProperties;
    private final ApplicationContext applicationContext;


    private static final Logger LOGGER = LoggerFactory.getLogger(SqsMessageHandlerConfig.class);

    public SqsMessageHandlerConfig(SqsMessageListenerListProperties sqsMessageListenerListProperties, ApplicationContext applicationContext) {
        this.sqsMessageListenerListProperties = sqsMessageListenerListProperties;
        this.applicationContext = applicationContext;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Bean(name = "sqsMessageHandlerMapping")
    public Map<String, Tuple2<Constructor<AbstractSqsMessageHandler<?>>, Method>> sqsMessageHandlerMapping() {
        final Map<String, Tuple2<Constructor<AbstractSqsMessageHandler<?>>, Method>> handlerMapping;
        // Those types in the noted package annotated with MessageHandler
//        final Reflections reflections = new Reflections(sqsMessageListenerListProperties.getHandlerBasePackage(), new SubTypesScanner(false), TypesAnnotated);
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(sqsMessageListenerListProperties.getHandlerBasePackage())));
        final Set<Class<AbstractSqsMessageHandler<?>>> annotated = (Set) reflections.get(TypesAnnotated.with(MessageHandler.class).asClass());
        // Create a List of Tuples: _1 is an instance of the 'Type' enum from SqsMessage -- this is pulled from the MessageHandler annotation
        //                          _2 is the class on which the MessageHandler annotation "sat".
        final List<Tuple2<String, Class<AbstractSqsMessageHandler<?>>>> tuples = annotated.stream()
                .map(a -> Tuple.of(a.getDeclaredAnnotation(MessageHandler.class).messageType(), a)).toList();

        // So now we populate this Map, which is from the 'Type' enum to a class constructor to create
        // an instance of the type on which the MessageHandler annotation sat.
        handlerMapping = tuples.stream().collect(Collectors.toMap(x -> x._1,
                tuple -> Tuple.of(Utils.getConstructor(tuple._2(), e -> {
                            LOGGER.error("*************** No empty constructor defined in " + tuple._2());
                            LOGGER.error("*************** No empty constructor defined in " + tuple._2());
                            LOGGER.error("All SQS Message Handlers must have an empty constructor [private/public/protected]");

                            throw new UtilsException("MISSING_EMPTY_CONSTRUCTOR", e);
                        }),
                        Utils.getMethod(AbstractSqsMessageHandler.class, "initialize", SqsMessage.class, String.class, String.class, Integer.class, Map.class, RateLimiter.class))));

        handlerMapping.forEach((key, value) -> LOGGER.info(String.format("handlerMapping1: %s + -> %s", key, value)));

        /*
            Will throw an NPE if there's no handlerMapping() entry for the given
            message type.
         */

        return handlerMapping;
    }

    @Bean(name = "sqsMethodLevelMessageHandlerMapping")
    public Map<String, Method> sqsMethodLevelMessageHandlerMapping() {
        final Map<String, Method> handlerMapping;
        // Those types in the noted package annotated with MessageHandler
//        final Reflections reflections = new Reflections(sqsMessageListenerListProperties.getHandlerBasePackage(), new SubTypesScanner(false), TypesAnnotated);
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(sqsMessageListenerListProperties.getHandlerBasePackage())).setScanners(MethodsAnnotated));
        final Set<Method> annotated = reflections.getMethodsAnnotatedWith(MessageHandler.class).stream().filter(this::isSpringBean).collect(Collectors.toSet());
        // Create a List of Tuples: _1 is an instance of the 'Type' enum from SqsMessage -- this is pulled from the MessageHandler annotation
        //                          _2 is the class on which the MessageHandler annotation "sat".
        final List<Tuple2<String, Method>> tuples = annotated.stream()
                .map(a -> Tuple.of(a.getDeclaredAnnotation(MessageHandler.class).messageType(), a)).toList();

        // So now we populate this Map, which is from the 'Type' enum to a class constructor to create
        // an instance of the type on which the MessageHandler annotation sat.
        handlerMapping = tuples.stream().collect(Collectors.toMap(Tuple2::_1,
                Tuple2::_2));

        handlerMapping.forEach((key, value) -> LOGGER.info(String.format("handlerMapping1: %s + -> %s", key, value)));

        /*
            Will throw an NPE if there's no handlerMapping() entry for the given
            message type.
         */

        return handlerMapping;
    }

    private boolean isSpringBean(final Method method) {
        final var declaringClass = method.getDeclaringClass();
        try {
            final var beanNames = applicationContext.getBeanNamesForType(declaringClass);

            if (ArrayUtils.isEmpty(beanNames)) {
                return false;
            } else {
                return true;
            }
        } catch (final Throwable ex) {
            log.error("method level @MessageHandler annotation can only be used in Spring beans.....");
            log.error("method level @MessageHandler annotation can only be used in Spring beans.....");

            return false;
        }
    }
}
