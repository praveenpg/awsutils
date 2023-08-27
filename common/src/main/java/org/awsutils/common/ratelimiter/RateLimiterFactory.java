package org.awsutils.common.ratelimiter;

import java.lang.reflect.Proxy;

public interface RateLimiterFactory {
    RateLimiter getRateLimiter(String name);

    static RateLimiterFactory getInstance() {
        return RateLimiterFactoryHelper.getRateLimiterFactory();
    }
}

@SuppressWarnings("unchecked")
class RateLimiterFactoryHelper {
    static Class<RateLimiterFactory> _class;
    static RateLimiterFactory RATE_LIMITER_FACTORY;

    public static RateLimiterFactory getRateLimiterFactory() {
        if(RATE_LIMITER_FACTORY == null) {
            synchronized (RateLimiterFactoryHelper.class) {
                if(RATE_LIMITER_FACTORY == null) {
                    try {
                        final RateLimiterFactory[] rateLimiterFactories;

                        _class = (Class<RateLimiterFactory>) Class.forName("org.awsutils.services.ratelimiter.config.RateLimiterFactoryImpl");
                        rateLimiterFactories = _class.getEnumConstants();
                        RATE_LIMITER_FACTORY = (RateLimiterFactory) Proxy.newProxyInstance(RateLimiterFactoryHelper.class.getClassLoader(), new Class[]{RateLimiterFactory.class},
                                (proxy, method, args) -> method.invoke(rateLimiterFactories[0], args));
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return RATE_LIMITER_FACTORY;
    }
}
