package org.awsutils.ratelimiter.config;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.ExpirationAfterWriteStrategy;
import io.github.bucket4j.redis.lettuce.cas.LettuceBasedProxyManager;
import io.lettuce.core.RedisClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.time.Duration;

public class RateLimiterAutoConfiguration {
    @ConditionalOnBean(LettuceConnectionFactory.class)
    public Bucket redisBucket(final LettuceConnectionFactory lettuceConnectionFactory) {
        final RedisClient redisClient = (RedisClient) lettuceConnectionFactory.getNativeClient();
        final LettuceBasedProxyManager proxyManager = LettuceBasedProxyManager.builderFor(redisClient)
                .withExpirationStrategy(ExpirationAfterWriteStrategy.basedOnTimeForRefillingBucketUpToMax(Duration.ofSeconds(10)))
                .build();;
        final BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.simple(1_000, Duration.ofMinutes(1)))
                .build();

        return proxyManager.builder().build(key, configuration);
    }
}
