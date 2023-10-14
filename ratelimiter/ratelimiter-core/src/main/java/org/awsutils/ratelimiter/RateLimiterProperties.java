package org.awsutils.ratelimiter;

import lombok.Data;

import java.util.List;

@Data
public class RateLimiterProperties {
    private RateLimiterType type;
    private List<RateLimit> limits;
}
