package org.awsutils.dynamodb.repositories;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Map;

@SuppressWarnings({"rawtypes", "SpringFacetCodeInspection"})
@Configuration
class DataMapperConfigInitializer {
    @Value("${org.awsutils.dynamodb.entities.basePackage:org.awsutils}")
    private String dtoBasePackage;

    private final Map<Class, DataMapper> dataMapperMap;
    private final Environment environment;


    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public DataMapperConfigInitializer(@Qualifier("dataMapperMap") final Map<Class, DataMapper> dataMapperMap, final Environment environment) {
        this.dataMapperMap = dataMapperMap;
        this.environment = environment;
    }

    @Bean
    DataMapperConfigCleanUp dataMapperConfigCleanUp() {
        return new DataMapperConfigCleanUp(dtoBasePackage, dataMapperMap, environment);
    }
}
