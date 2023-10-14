package org.awsutils.ratelimiter;

import java.lang.reflect.Proxy;

public interface RateLimiterFactory {
    RateLimiter getRateLimiter(String name);
    static RateLimiterFactory getInstance() {
        return RateLimiterFactoryHelper.getRateLimiterFactory();
    }
}

class RateLimiterFactoryHelper {
    static Class<RateLimiterFactory> _class;
    static RateLimiterFactory RATE_LIMITER_FACTORY;

    @SuppressWarnings("unchecked")
    public static RateLimiterFactory getRateLimiterFactory() {
        if(RATE_LIMITER_FACTORY == null) {
            synchronized (RateLimiterFactoryHelper.class) {
                if(RATE_LIMITER_FACTORY == null) {
                    try {
                        final RateLimiterFactory[] rateLimiterFactories;

                        _class = (Class<RateLimiterFactory>) Class.forName("org.awsutils.ratelimiter.impl.RateLimiterFactoryImpl");
                        rateLimiterFactories = _class.getEnumConstants();
                        RATE_LIMITER_FACTORY = (RateLimiterFactory) Proxy.newProxyInstance(RateLimiterFactoryHelper.class.getClassLoader(), new Class[]{RateLimiterFactory.class},
                                (proxy, method, args) -> method.invoke(rateLimiterFactories[0], args));
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        return RATE_LIMITER_FACTORY;
    }
}
