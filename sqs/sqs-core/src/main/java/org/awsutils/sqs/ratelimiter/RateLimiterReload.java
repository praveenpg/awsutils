package org.awsutils.sqs.ratelimiter;

public interface RateLimiterReload {
    void refreshIfRateChanged();
}
