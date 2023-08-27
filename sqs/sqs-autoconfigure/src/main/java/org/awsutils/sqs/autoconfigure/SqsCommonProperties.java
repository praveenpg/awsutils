package org.awsutils.sqs.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "org.awsutils.aws.sqs.common")
public class SqsCommonProperties {
    private boolean useCommonThreadPool;
    private int threadPoolSize = 25;
    private int threadPoolCoreSize = 25;
    private int taskExecutorThreadPoolSize = 5;
    private int maxThreadPoolQueueSize = 1000;
}
