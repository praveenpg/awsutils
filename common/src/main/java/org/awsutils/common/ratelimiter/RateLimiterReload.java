package org.awsutils.common.ratelimiter;

public interface RateLimiterReload {
    void refreshIfRateChanged();
}
