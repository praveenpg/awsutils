package org.awsutils.sqs.config;

import org.springframework.core.env.Environment;

public class LocalRateLimiter extends AbstractLocalRateLimiter {

    public LocalRateLimiter(final String rateLimiterKey, final String rateLimiterTimeUnitKey, final Environment environment) {
        super(rateLimiterKey, rateLimiterTimeUnitKey, environment);
    }

    @Override
    public String toString() {
        return "LocalRateLimiter{} " + super.toString();
    }
}
