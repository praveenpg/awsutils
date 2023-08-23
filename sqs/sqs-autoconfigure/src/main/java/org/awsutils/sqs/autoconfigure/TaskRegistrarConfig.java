package org.awsutils.sqs.autoconfigure;


import org.awsutils.sqs.listener.SqsMessageListener;
import org.awsutils.sqs.util.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

@SuppressWarnings({"SpringFacetCodeInspection"})
@Configuration
@ConditionalOnBean(value = {TaskScheduler.class})
public class TaskRegistrarConfig {
    private final TaskScheduler taskScheduler;

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskRegistrarConfig.class);

    public TaskRegistrarConfig(final TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    @Bean
    public SqsListenerScheduleConfig schedulingConfigurer() {
        LOGGER.info("Instantiating SqsListenerScheduleConfig");
        final SqsListenerScheduleConfig sqsListenerScheduleConfig = new SqsListenerScheduleConfigImpl(taskScheduler);

        return (SqsListenerScheduleConfig) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[]{SqsListenerScheduleConfig.class},
                (proxy, method, args) -> method.invoke(sqsListenerScheduleConfig, args));
    }

    private static class SqsListenerScheduleConfigImpl implements SqsListenerScheduleConfig {
        private final TaskScheduler taskScheduler;
        private final List<Tuple4<SqsMessageListener, String, String, Function<String, Integer>>> listenerList = new ArrayList<>();

        SqsListenerScheduleConfigImpl(final TaskScheduler taskScheduler) {
            this.taskScheduler = taskScheduler;
        }

        public void addListener(final SqsMessageListener sqsMessageListener, final String maxMessageKey, final String intervalKey, final Function<String, Integer> func1) {
            this.listenerList.add(Tuple4.of(sqsMessageListener, maxMessageKey, intervalKey, func1));
        }

        private Instant getNextRunTimeForPrefSyncUp(final TriggerContext triggerContext, final String intervalKey, final Function<String, Integer> func1) {
            return getNextExecutionTime(triggerContext, intervalKey, func1);
        }


        private Instant getNextExecutionTime(final TriggerContext triggerContext, final String propertyName, final Function<String, Integer> func1) {
            final Calendar nextExecutionTime = new GregorianCalendar();
            final Date lastActualExecutionTime = triggerContext.lastActualExecutionTime();

            nextExecutionTime.setTime(lastActualExecutionTime != null ? lastActualExecutionTime : new Date());
            nextExecutionTime.add(Calendar.MILLISECOND, func1.apply(propertyName));

            return nextExecutionTime.getTime().toInstant();
        }

        @Override
        public void configureTasks(final ScheduledTaskRegistrar scheduledTaskRegistrar) {
            scheduledTaskRegistrar.setTaskScheduler(taskScheduler);

            listenerList.forEach(tuple -> scheduledTaskRegistrar.addTriggerTask(() ->
                    tuple._1().receive(), triggerContext -> getNextRunTimeForPrefSyncUp(triggerContext, tuple._3(), tuple._4())));
        }
    }
}
