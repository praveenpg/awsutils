package org.awsutils.sqs.listener;

import org.awsutils.common.ratelimiter.RateLimiter;

import java.util.function.Supplier;

class PassthroughRateLimiter implements RateLimiter {
    @Override
    public boolean hasExceededMaxRate() {
        return false;
    }

    @Override
    public <T> T execute(Supplier<T> func0) {
        return func0.get();
    }

    @Override
    public void execute(Runnable action0) {
       action0.run();
    }

    @Override
    public int getMaxRate() {
        return Integer.MAX_VALUE;
    }

    @Override
    public String getRateLimiterName() {
        return "Pass through rate-limiter";
    }
}
