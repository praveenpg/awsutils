package org.awsutils.sqs.config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.awsutils.sqs.ratelimiter.RateLimiterReload;

public class RateLimiterReloader {
    private static final Logger LOGGER = LoggerFactory.getLogger(RateLimiterReloader.class);

    public RateLimiterReloader() {
        LOGGER.info("Constructing RateLimiterReloader");
    }

    @Scheduled(fixedRate = 30000)
    public void reloadRateLimiters() {
        RateLimiterFactoryImpl.INSTANCE.getAllRateLimiters().forEach(RateLimiterReload::refreshIfRateChanged);
    }
}
