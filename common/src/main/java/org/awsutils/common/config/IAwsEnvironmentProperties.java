package org.awsutils.common.config;

import java.time.Duration;

public interface IAwsEnvironmentProperties {
    String getRegion();

    String getAwsAccessKey();

    String getAwsAccessKeySecret();

    int getMaxConcurrency();

    boolean isLocalAwsMode();

    String getLocalAwsEndpoint();

    Duration getConnectionTimeout();

    Duration getConnectionMaxIdleTime();

    void setRegion(String region);

    void setAwsAccessKey(String awsAccessKey);

    void setAwsAccessKeySecret(String awsAccessKeySecret);

    void setMaxConcurrency(int maxConcurrency);

    void setLocalAwsMode(boolean localAwsMode);

    void setLocalAwsEndpoint(String localAwsEndpoint);

    void setConnectionTimeout(Duration connectionTimeout);

    void setConnectionMaxIdleTime(Duration connectionMaxIdleTime);
}
