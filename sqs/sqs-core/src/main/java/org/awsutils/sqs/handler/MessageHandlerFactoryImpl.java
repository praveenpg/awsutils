package org.awsutils.sqs.handler;



import io.vavr.Tuple2;
import org.apache.commons.lang3.ArrayUtils;
import org.awsutils.sqs.exceptions.UtilsException;
import org.awsutils.sqs.handler.impl.AbstractSqsMessageHandler;
import org.awsutils.sqs.handler.impl.MethodLevelSqsMessageHandler;
import org.awsutils.sqs.message.SqsMessage;
import org.awsutils.sqs.ratelimiter.RateLimiter;
import org.awsutils.sqs.util.ApplicationContextUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.*;
import java.util.Map;

@SuppressWarnings({"unchecked", "unused", "rawtypes"})
public class MessageHandlerFactoryImpl implements MessageHandlerFactory  {
    private final Map<String, Tuple2<Constructor<AbstractSqsMessageHandler>, Method>> handlerMapping;
    private final Map<String, Method> methodHandlerMapping;
    private final ApplicationContext applicationContext;

    public MessageHandlerFactoryImpl(final Map<String, Tuple2<Constructor<AbstractSqsMessageHandler>, Method>> handlerMapping,
                                     final Map<String, Method> methodHandlerMapping,
                                     final ApplicationContext applicationContext) {

        this.handlerMapping = handlerMapping;
        this.methodHandlerMapping = methodHandlerMapping;
        this.applicationContext = applicationContext;
    }


    @Override
    public <T> SqsMessageHandler<T> getMessageHandler(final String sqsMessage,
                                                      final String messageType,
                                                      final String transactionId,
                                                      final String receiptHandle,
                                                      final String queueUrl,
                                                      final Integer retryCount,
                                                      final Map<String, String> messageAttributes,
                                                      final RateLimiter messageHandlerRateLimiter) {

        if (!methodHandlerMapping.containsKey(messageType)) {
            throw new UtilsException("NO_HANDLER_FOR_MESSAGE_TYPE", String.format("No handler for message type '%s'",
                    messageType));
        }
        return getSqsMessageHandler(sqsMessage, messageType, transactionId, receiptHandle, queueUrl, retryCount, methodHandlerMapping.get(messageType), messageAttributes, messageHandlerRateLimiter);
    }

    @Override
    public <T> SqsMessageHandler<T> getMessageHandler(final SqsMessage<T> sqsMessage,
                                                      final String receiptHandle,
                                                      final String queueUrl,
                                                      final Integer retryCount,
                                                      final Map<String, String> messageAttributes,
                                                      final RateLimiter messageHandlerRateLimiter) {

        if (!handlerMapping.containsKey(sqsMessage.getMessageType())) {
            throw new UtilsException("NO_HANDLER_FOR_MESSAGE_TYPE", String.format("No handler for message type '%s'",
                    sqsMessage.getMessageType()));
        }

        return createSqsMessageHandlerProxy(getSqsMessageHandler(sqsMessage, receiptHandle, queueUrl,
                retryCount, handlerMapping.get(sqsMessage.getMessageType()), messageAttributes,
                messageHandlerRateLimiter));
    }

    private <T> SqsMessageHandler<T> getSqsMessageHandler(final String sqsMessage,
                                                          final String messageType,
                                                          final String transactionId,
                                                          final String receiptHandle,
                                                          final String queueUrl,
                                                          final Integer retryCount,
                                                          final Method handlerMethod,
                                                          final Map<String, String> messageAttributes,
                                                          final RateLimiter messageHandlerRateLimiter) {

        final MethodLevelSqsMessageHandler<T> sqsMessageHandler = new MethodLevelSqsMessageHandler(sqsMessage,
                transactionId,
                (Class) handlerMethod.getParameterTypes()[0],
                handlerMethod,
                applicationContext.getBean(handlerMethod.getDeclaringClass()),
                receiptHandle,
                queueUrl,
                retryCount,
                messageAttributes,
                messageHandlerRateLimiter);

        sqsMessageHandler.initializeForMethodLevelHandler(sqsMessage,
                transactionId,
                (Class) handlerMethod.getParameterTypes()[0],
                handlerMethod,
                receiptHandle,
                queueUrl,
                retryCount,
                messageAttributes,
                messageHandlerRateLimiter);

        return createSqsMessageHandlerProxy(sqsMessageHandler);
    }

    private <T> SqsMessageHandler<T> createSqsMessageHandlerProxy(SqsMessageHandler<T> sqsMessageHandler) {

        return (SqsMessageHandler<T>) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[]{SqsMessageHandler.class},
                (proxy, method, args) -> method.invoke(sqsMessageHandler, args));
    }

    private <T> SqsMessageHandler<T> getSqsMessageHandler(final SqsMessage<T> sqsMessage,
                                                          final String receiptHandle,
                                                          final String queueUrl,
                                                          final Integer retryCount,
                                                          final Tuple2<Constructor<AbstractSqsMessageHandler>, Method> constructor,
                                                          final Map<String, String> messageAttributes,
                                                          final RateLimiter messageHandlerRateLimiter) {
        try {
            final SqsMessageHandler<T> sqsMessageHandler = constructor._1().newInstance();

            constructor._2().invoke(sqsMessageHandler, sqsMessage, receiptHandle, queueUrl, retryCount, messageAttributes, messageHandlerRateLimiter);

            if(sqsMessageHandler.getClass().getAnnotation(Configurable.class) == null) {
                final Field[] fields = sqsMessageHandler.getClass().getDeclaredFields();

                if(!ArrayUtils.isEmpty(fields)) {
                    for (final Field field : fields) {
                        final Autowired autowired;
                        final Qualifier qualifier;
                        final Object bean;

                        field.setAccessible(true);

                        autowired = field.getAnnotation(Autowired.class);
                        qualifier = field.getAnnotation(Qualifier.class);

                        if (autowired != null) {
                            if (qualifier == null) {
                                bean = ApplicationContextUtils.getInstance().getBean(field.getType());
                            } else {
                                bean = ApplicationContextUtils.getInstance().getBean(field.getType(), qualifier.value());
                            }

                            field.set(sqsMessageHandler, bean);
                        }
                    }
                }
            }

            return sqsMessageHandler;
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new UtilsException("UNKNOWN_ERROR", String.format(
                    "Construction failed for message '%s' w/ queueUrl '%s' and retryCount '%d' using construction '%s' -- %s",
                    sqsMessage, queueUrl, retryCount, constructor._1().getName(), sqsMessage.getMessage()),
                    e);
        }
    }
}
