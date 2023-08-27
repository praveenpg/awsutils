package org.awsutils.sqs.config;


import org.awsutils.common.ratelimiter.RateLimiterFactory;
import org.awsutils.common.util.ApplicationContextUtils;
import org.awsutils.common.util.Utils;
import jakarta.annotation.PostConstruct;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


@SuppressWarnings("SpringFacetCodeInspection")
@Configuration
public class RateLimiterConfig {
    private final ApplicationContext applicationContext;
    private final Environment environment;

    public RateLimiterConfig(final ApplicationContext applicationContext, final Environment environment) {
        this.applicationContext = applicationContext;
        this.environment = environment;
    }

    @Bean
    public RateLimiterFactory rateLimiterFactory() {
        return RateLimiterFactoryImpl.INSTANCE;
    }

    @Bean
    public RateLimiterReloader rateLimiterReloader() {
        return new RateLimiterReloader();
    }

    @PostConstruct
    public void postConstruct() throws InvocationTargetException, IllegalAccessException {
        final Method method = Utils.getMethod(ApplicationContextUtils.class, "init", ApplicationContext.class, Environment.class);
        method.invoke(null, applicationContext, environment);
    }
}
