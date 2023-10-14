package org.awsutils.ratelimiter;

import lombok.Data;

import java.util.Map;

@Data
public class RateLimiterConfiguration {
    private Map<String, RateLimiterProperties> ratelimiters;
}
