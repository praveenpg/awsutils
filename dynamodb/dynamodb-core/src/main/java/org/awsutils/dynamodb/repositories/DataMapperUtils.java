package org.awsutils.dynamodb.repositories;

import org.awsutils.common.util.ApplicationContextUtils;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.Map;


@SuppressWarnings("unchecked")
final class DataMapperUtils {
    private DataMapperUtils(){}

    static <T> DataMapper<T> getDataMapper(final Class<T> paramType) {
        return (DataMapper<T>) getDataMapperMap().get(paramType);
    }

    static DynamoDbAsyncClient getDynamoDbAsyncClient() {
        return ApplicationContextUtils.getInstance().getBean(DynamoDbAsyncClient.class);
    }

    private static Map<Class<?>, ? extends DataMapper<?>> getDataMapperMap() {
        return ApplicationContextUtils.getInstance().getBean("dataMapperMap");
    }
}
