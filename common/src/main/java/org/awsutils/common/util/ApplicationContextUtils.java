package org.awsutils.common.util;

import lombok.Getter;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

@Getter
@SuppressWarnings("unchecked")
public class ApplicationContextUtils {
    private static volatile ApplicationContextUtils INSTANCE;
    private final ApplicationContext applicationContext;
    private final Environment environment;

    private ApplicationContextUtils(ApplicationContext applicationContext, Environment environment) {
        this.applicationContext = applicationContext;
        this.environment = environment;
    }

    public static ApplicationContextUtils getInstance() {
        return INSTANCE;
    }

    public <T> T getBean(final String name) {
        return (T) applicationContext.getBean(name);
    }

    public <T> T getBean(final Class<T> type) {
        return applicationContext.getBean(type);
    }

    public <T> T getBean(final Class<T> type, final String qualifierName) {
        return applicationContext.getBean(qualifierName, type);
    }

    @SuppressWarnings("unused")
    private static void init(final ApplicationContext applicationContext, final Environment environment) {
        if(INSTANCE == null) {
            synchronized (ApplicationContextUtils.class) {
                if(INSTANCE == null) {
                    INSTANCE = new ApplicationContextUtils(applicationContext, environment);
                }
            }
        }
    }
}
