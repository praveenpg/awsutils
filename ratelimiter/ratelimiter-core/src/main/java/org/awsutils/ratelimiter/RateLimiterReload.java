package org.awsutils.ratelimiter;

public interface RateLimiterReload {
    void refreshIfRateChanged();
}
