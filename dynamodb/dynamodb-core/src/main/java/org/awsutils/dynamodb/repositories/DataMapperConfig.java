package org.awsutils.dynamodb.repositories;


import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.awsutils.common.util.ApplicationContextUtils;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings({"SpringFacetCodeInspection", "rawtypes"})
@Configuration
public class DataMapperConfig {
    private final ApplicationContext applicationContext;
    private final Environment environment;

    public DataMapperConfig(final ApplicationContext applicationContext, final Environment environment) {
        this.applicationContext = applicationContext;
        this.environment = environment;
    }

    @Bean
    public Map<Class, DataMapper> dataMapperMap() {
        try {
            final Map<String, DataMapper> beans = applicationContext.getBeansOfType(DataMapper.class);
            final Method method = ApplicationContextUtils.class.getDeclaredMethod("init", ApplicationContext.class, Environment.class);

            method.setAccessible(true);

            method.invoke(null, applicationContext, environment);

            return new HashMap<>(beans.values().stream()
                    .map(bean -> Tuple.of(getParameterType(bean), bean))
                    .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2)));
        } catch (final Exception ex) {
            throw new BeanInitializationException("Exception while create dataMapperMap", ex);
        }
    }

    private Class getParameterType(final DataMapper<?> dataMapper) {
        return dataMapper.getParameterType();
    }
}
