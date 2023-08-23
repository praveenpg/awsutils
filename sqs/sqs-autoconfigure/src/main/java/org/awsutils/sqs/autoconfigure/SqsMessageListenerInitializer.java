package org.awsutils.sqs.autoconfigure;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.awsutils.sqs.client.SqsMessageClient;
import org.awsutils.sqs.config.WorkerNodeCheckFunc;
import org.awsutils.sqs.handler.MessageHandlerFactory;
import org.awsutils.sqs.listener.SqsMessageListener;
import org.awsutils.sqs.util.LimitedQueue;
import org.awsutils.sqs.util.Tuple2;
import org.awsutils.sqs.util.Utils;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.validation.ValidationException;
import jakarta.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.CollectionUtils;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings({"SpringFacetCodeInspection", "SpringJavaAutowiredFieldsWarningInspection", "unused", "ClassWithTooManyFields"})
@Configuration
@ConditionalOnBean(value = {
        TaskScheduler.class,
        SqsMessageClient.class,
        MessageHandlerFactory.class,
        SqsAsyncClient.class
})
public class SqsMessageListenerInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsMessageListenerInitializer.class);
    private final SqsMessageListenerListProperties sqsMessageListenerListProperties;
    private final SqsCommonProperties sqsCommonProperties;
    private final ApplicationContext applicationContext;
    private final SqsConfig.SqsPropertyFunc1<String, Integer> propertyFunc;
    private final MessageHandlerFactory messageHandlerFactory;
    private final SqsMessageClient sqsMessageClient;
    private final SqsAsyncClient sqsAsyncClient;
    private final SqsListenerScheduleConfig schedulingConfigurer;
    private final Environment environment;
    private static final Integer DEFAULT_WAIT_TIME_IN_SECONDS = 10;
    private final List<ExecutorService> executorServices = new ArrayList<>();

    @Autowired(required = false)
    private SqsConfig.CommonExecutorService commonExecutorService;

    @Autowired(required = false)
    private WorkerNodeCheckFunc workerNodeCheckFunc;

    private static final String SQS_MESSAGE_LISTENER_KEY = "sqsMessageListener_{0}";


    public SqsMessageListenerInitializer(final SqsMessageListenerListProperties sqsMessageListenerListProperties,
                                         final SqsCommonProperties sqsCommonProperties,
                                         final ApplicationContext applicationContext,
                                         final SqsConfig.SqsPropertyFunc1<String, Integer> propertyFunc,
                                         final MessageHandlerFactory messageHandlerFactory,
                                         @Qualifier("sqsMessageClient") final SqsMessageClient sqsMessageClient,
                                         final SqsAsyncClient sqsAsyncClient,
                                         final SqsListenerScheduleConfig schedulingConfigurer,
                                         final Environment environment) {

        this.sqsMessageListenerListProperties = sqsMessageListenerListProperties;
        this.sqsCommonProperties = sqsCommonProperties;
        this.applicationContext = applicationContext;
        this.propertyFunc = propertyFunc;
        this.messageHandlerFactory = messageHandlerFactory;
        this.sqsMessageClient = sqsMessageClient;
        this.sqsAsyncClient = sqsAsyncClient;
        this.schedulingConfigurer = schedulingConfigurer;
        this.environment = environment;
    }

    @PostConstruct
    public void init() {
        if(sqsMessageListenerListProperties != null) {
            final Map<String, SqsMessageListenerProperties> listenerMap = sqsMessageListenerListProperties.getListener();

            if (!CollectionUtils.isEmpty(listenerMap)) {
                final AutowireCapableBeanFactory autowireBeanFactory = applicationContext.getAutowireCapableBeanFactory();
                final BeanDefinitionRegistry registry = (BeanDefinitionRegistry) autowireBeanFactory;

                listenerMap.keySet().forEach(listenerKey -> registerSqsListener(registry, listenerKey, listenerMap.get(listenerKey)));
            }
        }
    }

    @SuppressWarnings({"UnusedAssignment", "DuplicatedCode"})
    public void registerSqsListener(final BeanDefinitionRegistry registry, final String listenerKey, final SqsMessageListenerProperties sqsMessageListenerProperties) {
        try {
            final GenericBeanDefinition definition = new GenericBeanDefinition();
            final ConstructorArgumentValues constructorArgumentValues = new ConstructorArgumentValues();
            final String listenerName = sqsMessageListenerProperties.getListenerName();
            final String beanName = MessageFormat.format(SQS_MESSAGE_LISTENER_KEY, listenerKey);
            final String rateLimiterName = sqsMessageListenerProperties.getRateLimiterName();
            final String listenerEnabledProperty = sqsMessageListenerProperties.getStatusProperty();
            final Integer waitTimeInSeconds = sqsMessageListenerProperties.getWaitTimeInSeconds();
            final String messageHandlerRateLimiterName = sqsMessageListenerProperties.getMessageHandlerRateLimiterName();
            int index = 0;
            final WorkerNodeCheckFunc finalWorkerNodeCheckFunc = workerNodeCheckFunc == null ?
                    () -> StringUtils.isEmpty(listenerEnabledProperty) || isSqsListenerEnabled(listenerEnabledProperty) :
                    () -> (StringUtils.isEmpty(listenerEnabledProperty) || isSqsListenerEnabled(listenerEnabledProperty)) && workerNodeCheckFunc.check();
            final Function<Integer, SqsMessageListener> sqsMessageListenerFunc = c -> SqsMessageListener
                    .builder()
                    .sqsAsyncClient(sqsAsyncClient)
                    .queueName(sqsMessageListenerProperties.getQueueName())
                    .sqsMessageClient(sqsMessageClient)
                    .messageHandlerFactory(messageHandlerFactory)
                    .executorService(sqsCommonProperties.isUseCommonThreadPool() ? commonExecutorService.executorService() : createExecutorService(sqsMessageListenerProperties.getThreadPoolSize()))
                    .rateLimiterName(!StringUtils.isEmpty(rateLimiterName) ? rateLimiterName : null)
                    .maximumNumberOfMessagesKey(sqsMessageListenerProperties.getMaximumNumberOfMessagesKey())
                    .semaphore(new Semaphore(1))
                    .propertyReaderFunction(propertyFunc)
                    .workerNodeCheck(finalWorkerNodeCheckFunc)
                    .listenerName(!StringUtils.isEmpty(listenerName) ? String.format("%s_%d", listenerName, c) : String.format("%s_%d", listenerKey, c))
                    .messageHandlerRateLimiter(!StringUtils.isEmpty(messageHandlerRateLimiterName) ? messageHandlerRateLimiterName : null)
                    .statusProperty(!StringUtils.isEmpty(listenerEnabledProperty) ? listenerEnabledProperty : null)
                    .waitTimeInSeconds(waitTimeInSeconds != null && waitTimeInSeconds > 0 ? waitTimeInSeconds : DEFAULT_WAIT_TIME_IN_SECONDS)
                    .queueUrl(sqsMessageListenerProperties.getQueueUrl())
                    .build();

            validate(sqsMessageListenerProperties);

            definition.setBeanClassName("org.awsutils.sqs.autoconfigure.SqsMessageListenerInitializer.SqsMessageListenerWrapper");

            constructorArgumentValues.addIndexedArgumentValue(index++, executorServices);
            constructorArgumentValues.addIndexedArgumentValue(index++, sqsMessageListenerProperties.getNumberOfListenersProperty());
            constructorArgumentValues.addIndexedArgumentValue(index++, environment);
            constructorArgumentValues.addIndexedArgumentValue(index++, sqsMessageListenerFunc);
            constructorArgumentValues.addIndexedArgumentValue(index++, !StringUtils.isEmpty(listenerName) ? listenerName : listenerKey);

            definition.setConstructorArgumentValues(constructorArgumentValues);

            registry.registerBeanDefinition(beanName, definition);

            schedulingConfigurer.addListener((SqsMessageListener) applicationContext.getBean(beanName),
                    sqsMessageListenerProperties.getMaximumNumberOfMessagesKey(),
                    sqsMessageListenerProperties.getScheduleRunIntervalKey(), propertyFunc);
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isSqsListenerEnabled(final String statusPropertyName) {
        final Boolean enabled = environment.getProperty(statusPropertyName, Boolean.class);

        return enabled == null || enabled;
    }

    private ExecutorService createExecutorService(final int fixedThreadPoolSize) {
        synchronized (this) {
            final LimitedQueue<Runnable> runnables = new LimitedQueue<>(1000);
            final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(fixedThreadPoolSize, fixedThreadPoolSize,
                    60L, TimeUnit.SECONDS,
                    runnables, new LimitedQueue.LimitedQueueRejectedExecutionPolicy()){
                @Override
                public void shutdown() {
                    LOGGER.info("Shutting down executor service");
                    super.shutdown();
                }
            };

            runnables.setThreadPoolExecutor(threadPoolExecutor);

            executorServices.add(threadPoolExecutor);

            return threadPoolExecutor;
        }
    }

    @PreDestroy
    public void cleanUp() {
        executorServices.forEach(executorService -> {
            try {
                executorService.shutdown();
            } catch (Exception ignored){}
        });
    }

    @SuppressWarnings("ConstantConditions")
    private static class SqsMessageListenerWrapper implements SqsMessageListener {
        private static final long SEMAPHORE_TIMEOUT_IN_SECONDS = 15L;
        private final Environment environment;
        private final String numberOfListenersProperty;
        private final Function<Integer, SqsMessageListener> sqsMessageListenerFunc;
        private final List<ExecutorService> executorServices;
        private final String listenerName;
        private List<SqsMessageListener> sqsMessageListeners;
        private ExecutorService executorService;
        private Semaphore semaphore;
        private long lastCheckedTime;
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private static final long TEN_MINUTES_IN_MILLIS = TimeUnit.MINUTES.toMillis(10);

        public SqsMessageListenerWrapper(final List<ExecutorService> executorServices,
                                         final String numberOfListenersProperty,
                                         final Environment environment,
                                         final Function<Integer, SqsMessageListener> sqsMessageListenerFunc,
                                         final String listenerName) {

            final int numberOfListeners;

            this.environment = environment;
            this.numberOfListenersProperty = numberOfListenersProperty;
            this.sqsMessageListenerFunc = sqsMessageListenerFunc;
            this.listenerName = listenerName;

            numberOfListeners = getNumberOfListeners();
            this.semaphore = new Semaphore(numberOfListeners);
            this.sqsMessageListeners = new ArrayList<>();
            this.sqsMessageListeners = IntStream.range(0, numberOfListeners).boxed().map(sqsMessageListenerFunc::apply).collect(Collectors.toList());
            this.lastCheckedTime = System.currentTimeMillis();

            synchronized (SqsMessageListenerWrapper.class) {
                this.executorService = Executors.newFixedThreadPool(numberOfListeners);
                this.executorServices = executorServices;

                executorServices.add(executorService);
            }
        }

        @Override
        public void receive() {
            final long startTime = System.currentTimeMillis();

            if((startTime - lastCheckedTime) >= TEN_MINUTES_IN_MILLIS) {
                checkForUpdates();
                this.lastCheckedTime = startTime;
            }
            Utils.executeUsingLock(lock.readLock(), () -> sqsMessageListeners.stream().map(this::submitJobToListener).forEach(this::waitForCompletion));
        }

        private void checkForUpdates() {
            final int numberOfListeners = getNumberOfListeners();

            if(numberOfListeners != sqsMessageListeners.size()) {
                LOGGER.info("Number of listeners have changed for {} from {} to {}", this.listenerName, this.sqsMessageListeners.size(), numberOfListeners);

                Utils.executeUsingLock(lock.writeLock(), () -> {
                    this.sqsMessageListeners = IntStream.range(0, numberOfListeners).boxed().map(sqsMessageListenerFunc::apply).collect(Collectors.toList());
                    this.semaphore = new Semaphore(numberOfListeners);

                    synchronized (SqsMessageListenerWrapper.class) {
                        this.executorServices.remove(this.executorService);
                        this.executorService.shutdown();
                        this.executorService = Executors.newFixedThreadPool(this.sqsMessageListeners.size());
                        this.executorServices.add(this.executorService);
                    }
                });
            }
        }

        private int getNumberOfListeners() {
            return !StringUtils.isEmpty(numberOfListenersProperty) ? environment.getProperty(numberOfListenersProperty, Integer.class) : 1;
        }

        public void waitForCompletion(final Tuple2<Boolean, Future<?>> future) {
            try {
                future._2().get();
            } catch (final InterruptedException e) {
                Utils.handleInterruptedException(e, () -> {});
            } catch (final ExecutionException e) {
                final Exception ex = (Exception) e.getCause();

                throw (ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex));
            } finally {
                if(future._1()) {
                    semaphore.release();
                }
            }
        }

        public Tuple2<Boolean, Future<?>> submitJobToListener(final SqsMessageListener a) {
            try {
                boolean lockAcquired = semaphore.tryAcquire(SEMAPHORE_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);

                if(lockAcquired) {
                    return Tuple2.of(true, executorService.submit(a::receive));
                } else {
                    return Tuple2.of(false, CompletableFuture.completedFuture(null));
                }
            } catch (final InterruptedException ex) {
                return Utils.handleInterruptedException(ex, () -> Tuple2.of(false, CompletableFuture.completedFuture(null)));
            }
        }
    }

    public static void validate(final Object a) throws IllegalAccessException, JsonProcessingException {
        final Field[] fields = a.getClass().getDeclaredFields();
        final List<String> errorList = new ArrayList<>();
        final Map<String, Object> errorMap = new HashMap<>();

        for (final Field field : fields) {
            if(!Modifier.isStatic(field.getModifiers())) {
                final NotNull notNull = field.getAnnotation(NotNull.class);

                if(notNull != null) {
                    final Object value;

                    field.setAccessible(true);
                    value = field.get(a);

                    if(value == null) {
                        errorList.add(notNull.message());
                    }
                }
            }
        }

        if(!CollectionUtils.isEmpty(errorList)) {

            errorMap.put("message", "Following fields have not been populated");

            errorMap.put("fields", errorList);

            logErrorMessageToConsole(errorList);

            throw new ValidationException(Utils.constructJson(errorMap));
        }
    }

    private static void logErrorMessageToConsole(final List<String> errorList) {
        final AtomicInteger counter = new AtomicInteger(0);
        System.err.println();
        System.err.println();
        System.err.println();
        System.err.println("####################### ALL REQUIRED PROPERTIES NOT POPULATED - Stopping Application #######################");
        System.err.println();
        System.err.println("Following fields not populated, Please add to configuration property/yaml file: ");
        errorList.forEach(a -> System.err.println(counter.incrementAndGet() + ": " + a));
        System.err.println();
        System.err.println("####################### ALL REQUIRED PROPERTIES NOT POPULATED - Stopping Application #######################");
        System.err.println();
        System.err.println();
        System.err.println();
    }
}
