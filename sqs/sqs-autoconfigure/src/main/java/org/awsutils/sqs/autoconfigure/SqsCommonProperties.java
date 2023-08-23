package org.awsutils.sqs.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.pearson.aws.sqs.common")
public class SqsCommonProperties {
    private boolean useCommonThreadPool;
    private int threadPoolSize = 25;
    private int threadPoolCoreSize = 25;
    private int taskExecutorThreadPoolSize = 5;
    private int maxThreadPoolQueueSize = 1000;

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(final int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public boolean isUseCommonThreadPool() {
        return useCommonThreadPool;
    }

    public void setUseCommonThreadPool(final boolean useCommonThreadPool) {
        this.useCommonThreadPool = useCommonThreadPool;
    }

    public int getTaskExecutorThreadPoolSize() {
        return taskExecutorThreadPoolSize;
    }

    public void setTaskExecutorThreadPoolSize(final int taskExecutorThreadPoolSize) {
        this.taskExecutorThreadPoolSize = taskExecutorThreadPoolSize;
    }

    public int getMaxThreadPoolQueueSize() {
        return maxThreadPoolQueueSize;
    }

    public void setMaxThreadPoolQueueSize(final int maxThreadPoolQueueSize) {
        this.maxThreadPoolQueueSize = maxThreadPoolQueueSize;
    }

    public int getThreadPoolCoreSize() {
        return threadPoolCoreSize;
    }

    public void setThreadPoolCoreSize(final int threadPoolCoreSize) {
        this.threadPoolCoreSize = threadPoolCoreSize;
    }
}
