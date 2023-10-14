package org.awsutils.sqs.config;


import org.awsutils.common.exceptions.UtilsException;
import org.awsutils.common.ratelimiter.RateLimiter;
import org.awsutils.common.ratelimiter.RateLimiterReload;
import org.awsutils.common.util.Utils;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@SuppressWarnings("UnstableApiUsage")
public abstract class AbstractLocalRateLimiter implements RateLimiter, RateLimiterReload {
    private final Environment environment;
    private final String rateLimiterTimeUnitKey;
    private final String rateLimiterKey;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private int maxRate;
    private com.google.common.util.concurrent.RateLimiter rateLimiter;

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLocalRateLimiter.class);
    private static final long SLEEP_TIME = 10;

    public AbstractLocalRateLimiter(final String rateLimiterKey, final String rateLimiterTimeUnitKey, final Environment environment) {
        this.environment = environment;
        this.rateLimiterTimeUnitKey = rateLimiterTimeUnitKey;
        this.rateLimiterKey = rateLimiterKey;
    }


    @Override
    public boolean hasExceededMaxRate() {
        return Utils.executeUsingLock(lock.readLock(), () -> {
            try {

                while (hasExceededRateLimit()) {
                }

                return false;
            } catch (Exception e) {
                LOGGER.warn("Exception in rate limiter: " + e, e);
                return true;
            }
        });
    }

    private boolean hasExceededRateLimit() throws InterruptedException {
        if (rateLimiter != null) {
            final long startTime = System.currentTimeMillis();
            final boolean rateLimitExceeded = rateLimiter.tryAcquire(SLEEP_TIME, TimeUnit.MILLISECONDS);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("RATE_LIMIT_CHECK_TIME_TAKEN: " + (System.currentTimeMillis() - startTime));
            }

            return !rateLimitExceeded;
        } else {
            Thread.sleep(SLEEP_TIME);
            return Boolean.TRUE;
        }
    }

    @Override
    public int getMaxRate() {
        return this.maxRate;
    }

    @Override
    public String getRateLimiterName() {
        return rateLimiterKey;
    }

    @PostConstruct
    public AbstractLocalRateLimiter init() {
        createRateLimiter(
                Integer.parseInt(environment.getProperty(rateLimiterKey, "25")),
                TimeUnit.valueOf(environment.getProperty(rateLimiterTimeUnitKey, TimeUnit.SECONDS.name())));

        return this;
    }

    private void createRateLimiter(final int rateLimit, final TimeUnit timeUnit) {
        final long time = timeUnit.toSeconds(1);
        final int rate = (int) (rateLimit / time);

        if (rate < 1) {
            throw new UtilsException("Rate cannot be lower than 0 per second");
        }

        this.maxRate = rate;

        this.rateLimiter = com.google.common.util.concurrent.RateLimiter.create(rate);
    }

    /**
     * Checks the instance level rate value against the value in FF4J and reloads the RateLimiter instance if they are different.
     */
    @Override
    public void refreshIfRateChanged() {
        final int configuredMaxRate = Integer.parseInt(environment.getProperty(rateLimiterKey, "25"));
        final TimeUnit timeUnit = TimeUnit.valueOf(environment.getProperty(rateLimiterTimeUnitKey, TimeUnit.SECONDS.name()));

        if (configuredMaxRate != this.maxRate && configuredMaxRate > 0) {
            Utils.executeUsingLock(lock.writeLock(), () -> {
                if (configuredMaxRate != this.maxRate) {
                    createRateLimiter(configuredMaxRate, timeUnit);
                }
            });
        }
    }
}
