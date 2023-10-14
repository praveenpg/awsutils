package org.awsutils.ratelimiter;

import lombok.Data;

import java.time.Duration;

@Data
public class RateLimit {
    private int limit;
    private Duration duration;
}
