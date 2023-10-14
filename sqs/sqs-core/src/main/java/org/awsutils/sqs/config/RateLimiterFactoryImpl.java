package org.awsutils.sqs.config;


import org.awsutils.common.exceptions.UtilsException;
import org.awsutils.common.ratelimiter.RateLimiter;
import org.awsutils.common.ratelimiter.RateLimiterFactory;
import org.awsutils.common.ratelimiter.RateLimiterReload;
import org.awsutils.common.ratelimiter.RateLimiterType;
import org.awsutils.common.util.ApplicationContextUtils;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

import java.lang.reflect.Proxy;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

enum RateLimiterFactoryImpl implements RateLimiterFactory {
    INSTANCE;

    private final ConcurrentHashMap<String, RateLimiter> localRateLimiters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RateLimiter> distributedRateLimiters = new ConcurrentHashMap<>();
    private static final String RATE_LIMITER_CONFIG_PREFIX = "org.awsutils.ratelimiter.config.ratelimiters.{0}.";

    @Override
    public RateLimiter getRateLimiter(final String name) {
        final Environment environment = ApplicationContextUtils.getInstance().getEnvironment();
        final String rateLimiterTypeStr = environment.getProperty(MessageFormat.format(RATE_LIMITER_CONFIG_PREFIX + "type", name));
        final RateLimiterType rateLimiterType = !StringUtils.isEmpty(rateLimiterTypeStr) ? RateLimiterType.valueOf(rateLimiterTypeStr) : RateLimiterType.LOCAL;

//        return getRateLimiter(name, rateLimiterType == RateLimiterType.DISTRIBUTED ? ApplicationContextUtils.getInstance().getBean(RedisClient.class) : null, environment);
        return getRateLimiter(name, environment);
    }

    public RateLimiter getRateLimiter(final String name,
//                                      final RedisClient redisClient,
                                      final Environment environment) {
        final String rateLimiterTypeStr = environment.getProperty(MessageFormat.format(RATE_LIMITER_CONFIG_PREFIX + "type", name));
        final RateLimiterType rateLimiterType = !StringUtils.isEmpty(rateLimiterTypeStr) ? RateLimiterType.valueOf(rateLimiterTypeStr) : RateLimiterType.LOCAL;
        final Integer maxPerTimeUnit = environment.getProperty(MessageFormat.format(RATE_LIMITER_CONFIG_PREFIX + "maxRate", name), Integer.class);
        final String maxRateTimeUnitKey = MessageFormat.format(RATE_LIMITER_CONFIG_PREFIX + "maxRateTimeUnit", name);

        if(maxPerTimeUnit != null && maxPerTimeUnit < 0) {
            throw new UtilsException("Invalid maxRate value for " + name);
        }

//        if(rateLimiterType == RateLimiterType.LOCAL) {
            return localRateLimiters.computeIfAbsent(name, s -> getRateLimiterProxy(new LocalRateLimiter(MessageFormat.format(RATE_LIMITER_CONFIG_PREFIX + "maxRate", name), maxRateTimeUnitKey, environment).init()));
//        } else {
//            return distributedRateLimiters.computeIfAbsent(name, s -> getRateLimiterProxy(new DistributedRateLimiter(MessageFormat.format(RATE_LIMITER_CONFIG_PREFIX + "maxRate", name),
//                    maxRateTimeUnitKey, redisClient,
//                    environment).init()));
//        }
    }


    public RateLimiter getRateLimiterProxy(final RateLimiter rateLimiter) {
        return (RateLimiter) Proxy.newProxyInstance(RateLimiterFactoryImpl.class.getClassLoader(), new Class[]{RateLimiter.class, RateLimiterReload.class},
                (proxy, method, args) -> method.invoke(rateLimiter, args));
    }

    public List<RateLimiterReload> getAllRateLimiters() {
        return Stream.concat(localRateLimiters.values().stream().map(a -> (RateLimiterReload) a),
                distributedRateLimiters.values().stream().map(b -> (RateLimiterReload) b)).collect(Collectors.toList());
    }
}
