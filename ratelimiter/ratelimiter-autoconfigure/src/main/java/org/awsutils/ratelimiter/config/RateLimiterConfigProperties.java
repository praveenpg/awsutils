package org.awsutils.ratelimiter.config;

import lombok.Data;
import org.awsutils.ratelimiter.RateLimiterProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "org.awsutils.ratelimiter")
public class RateLimiterConfigProperties {
    private boolean enableDistributedRateLimiting = false;
    private RateLimiterProperties rateLimiterProperties;
}
