package org.awsutils.dynamodb.repositories;




import io.vavr.Tuple2;
import org.awsutils.dynamodb.annotations.DbAttribute;
import org.awsutils.dynamodb.annotations.KeyType;
import org.springframework.util.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Map;

@lombok.Getter
@SuppressWarnings({"unused", "WeakerAccess", "RedundantSuppression"})
final class AttributeMapper<T> {
    private final Class<T> mappedClass;
    private final Map<String, Tuple2<Field, DbAttribute>> mappedFields;
    private final Constructor<T> constructor;
    private final Map<KeyType, Tuple2<String, Field>> primaryKeyMapping;
    private final Map<String, GSI> globalSecondaryIndexMap;
    private final String tableName;
    private final Tuple2<String, Field> dateUpdatedField;
    private final Tuple2<String, Field> dateCreatedField;
    private final Tuple2<Field, DbAttribute> versionAttributeField;

    private AttributeMapper(final Class<T> mappedClass,
                            final Map<String, Tuple2<Field, DbAttribute>> mappedFields,
                            final Constructor<T> constructor,
                            final Map<KeyType, Tuple2<String, Field>> primaryKeyMapping,
                            final String tableName,
                            final Tuple2<String, Field> dateUpdatedField,
                            final Tuple2<String, Field> dateCreatedField,
                            final Map<String, GSI> globalSecondaryIndexMap,
                            final Tuple2<Field, DbAttribute> versionAttributeField) {

        this.mappedClass = mappedClass;
        this.mappedFields = mappedFields;
        this.constructor = constructor;
        this.primaryKeyMapping = primaryKeyMapping;
        this.tableName = !StringUtils.isEmpty(tableName) ? tableName : mappedClass.getSimpleName();
        this.dateUpdatedField = dateUpdatedField;
        this.dateCreatedField = dateCreatedField;
        this.globalSecondaryIndexMap = globalSecondaryIndexMap;
        this.versionAttributeField = versionAttributeField;
    }


    public static<T> Builder<T> builder() {
        return new AttributeMapperBuilder<>();
    }

    @SuppressWarnings("UnusedReturnValue")
    public interface Builder<T> {
        Builder<T> mappedClass(Class<T> mappedClass);

        Builder<T> mappedFields(Map<String, Tuple2<Field, DbAttribute>> mappedFields);

        Builder<T> constructor(Constructor<T> constructor);
        Builder<T> primaryKeyMapping(Map<KeyType, Tuple2<String, Field>> primaryKeyMapping);
        Builder<T> tableName(String tableName);
        Builder<T> dateUpdatedField(Tuple2<String, Field> dateUpdatedField);
        Builder<T> dateCreatedField(Tuple2<String, Field> dateCreatedField);
        Builder<T> globalSecondaryIndexMap(Map<String, GSI> globalSecondaryIndexMap);
        Builder<T> versionAttributeField(Tuple2<Field, DbAttribute> versionAttributeField);

        AttributeMapper<T> build();
    }

    private static class AttributeMapperBuilder<T> implements Builder<T> {
        private Class<T> mappedClass;
        private Map<String, Tuple2<Field, DbAttribute>> mappedFields;
        private Constructor<T> constructor;
        private Map<KeyType, Tuple2<String, Field>> primaryKeyMapping;
        private String tableName;
        private Tuple2<String, Field> dateUpdatedField;
        private Tuple2<String, Field> dateCreatedField;
        private Map<String, GSI> globalSecondaryIndexMap;
        private Tuple2<Field, DbAttribute> versionAttributeField;

        @Override
        public Builder<T> mappedClass(final Class<T> mappedClass) {
            this.mappedClass = mappedClass;
            return this;
        }

        @Override
        public Builder<T> mappedFields(final Map<String, Tuple2<Field, DbAttribute>> mappedFields) {
            this.mappedFields = mappedFields;
            return this;
        }

        @Override
        public Builder<T> constructor(final Constructor<T> constructor) {
            this.constructor = constructor;
            return this;
        }

        @Override
        public Builder<T> primaryKeyMapping(final Map<KeyType, Tuple2<String, Field>> primaryKeyMapping) {
            this.primaryKeyMapping = primaryKeyMapping;
            return this;
        }

        @Override
        public Builder<T> tableName(final String tableName) {
            this.tableName = tableName;
            return this;
        }

        @Override
        public Builder<T> dateUpdatedField(final Tuple2<String, Field> dateUpdatedField) {
            this.dateUpdatedField = dateUpdatedField;
            return this;
        }

        @Override
        public Builder<T> dateCreatedField(final Tuple2<String, Field> dateCreatedField) {
            this.dateCreatedField = dateCreatedField;
            return this;
        }

        @Override
        public Builder<T> globalSecondaryIndexMap(final Map<String, GSI> globalSecondaryIndexMap) {
            this.globalSecondaryIndexMap = globalSecondaryIndexMap;

            return this;
        }

        @Override
        public Builder<T> versionAttributeField(final Tuple2<Field, DbAttribute> versionAttributeField) {
            this.versionAttributeField = versionAttributeField;

            return this;
        }

        @Override
        public AttributeMapper<T> build() {
            return new AttributeMapper<>(mappedClass,
                    mappedFields,
                    constructor,
                    primaryKeyMapping,
                    tableName,
                    dateUpdatedField,
                    dateCreatedField,
                    globalSecondaryIndexMap, versionAttributeField);
        }
    }
}
