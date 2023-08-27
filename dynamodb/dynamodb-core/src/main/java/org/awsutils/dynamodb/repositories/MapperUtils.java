package org.awsutils.dynamodb.repositories;

import com.google.common.collect.ImmutableList;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.Tuple4;
import org.awsutils.common.exceptions.UtilsException;
import org.awsutils.common.util.PropertyResolverUtils;
import org.awsutils.common.util.Utils;
import org.awsutils.dynamodb.annotations.*;
import org.awsutils.dynamodb.data.VersionedEntity;
import org.awsutils.dynamodb.exceptions.DbException;
import org.springframework.core.env.Environment;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({"unchecked", "DuplicatedCode"})
public enum MapperUtils {
    INSTANCE;
    private final ConcurrentHashMap<String, AttributeMapper<?>> attributeMappingMap = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private <T> List<Field> getAllFields(final Class<T> dataClass) {
        return getAllFields(dataClass, ImmutableList.of());
    }

    @SuppressWarnings({"ConstantConditions", "rawtypes"})
    private List<Field> getAllFields(final Class dataClass, final List<Field> acc) {
        if(dataClass != VersionedEntity.class && dataClass != Object.class) {
            final Field[] fields = dataClass.getDeclaredFields();

            return getAllFields(dataClass.getSuperclass(), ImmutableList.<Field>builder().addAll(acc)
                    .addAll((fields != null && fields.length > 0) ? Arrays.asList(fields) : Collections.emptyList()).build());
        } else if(dataClass == VersionedEntity.class){
            return filterFields(ImmutableList.<Field>builder()
                    .addAll(acc)
                    .add(ReflectionUtils.findField(VersionedEntity.class, "version"))
                    .build());
        } else {
            return filterFields(acc);
        }
    }

    private List<Field> filterFields(final List<Field> fields) {
        return fields.stream()
                .filter(field -> !field.getName().contains("ajc$"))
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .collect(Collectors.toList());
    }

    <T> void setDbAttributes(final Class<T> dataClass, final Environment environment, final DataMapper<T> dataMapper) {
        Utils.executeUsingLock(lock.writeLock(), () -> {
            final String tableName;
            final Constructor<T> constructor = constructor(dataClass);
            final DDBTable table = dataClass.getAnnotation(DDBTable.class);
            final List<Field> fields = getAllFields(dataClass);
            final Map<String, Tuple2<Field, DbAttribute>> map = new HashMap<>();
            final Map<KeyType, Tuple2<String, Field>> primaryKeyMapping = new HashMap<>();
            final Map<String, Tuple2<Field, DbAttribute>> versionAttMap = new HashMap<>();
            final ConcurrentHashMap<String, GSI.Builder> indexMap = new ConcurrentHashMap<>();
            final AttributeMapper.Builder<T> builder;

            if (table != null) {
                tableName = PropertyResolverUtils.getEnvironmentProperty(table.name(), environment);
            } else {
                tableName = dataMapper.tableName();
            }

            builder = AttributeMapper.builder();

            fields.forEach(field -> setFieldMappings(map, primaryKeyMapping, field, indexMap,
                    versionAttMap, builder));

            if (!CollectionUtils.isEmpty(versionAttMap) && versionAttMap.size() > 1) {
                throw new DbException("Entity cannot have more than one version attribute");
            }

            if(CollectionUtils.isEmpty(primaryKeyMapping) || primaryKeyMapping.get(KeyType.HASH_KEY) == null) {
                throw new DbException(String.format("Entity class %s does not have a hash key defined", dataClass.getName()));
            }

            attributeMappingMap.put(dataClass.getName(), builder.mappedClass(dataClass)
                    .mappedFields(map)
                    .constructor(constructor)
                    .primaryKeyMapping(primaryKeyMapping)
                    .globalSecondaryIndexMap(indexMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, b -> b.getValue().build())))
                    .versionAttributeField(!CollectionUtils.isEmpty(versionAttMap) ? versionAttMap.entrySet().iterator().next().getValue() : null)
                    .tableName(tableName)
                    .build());
        });
    }

    <T> Stream<Tuple4<String, Object, Field, DbAttribute>> getMappedValues(final T input, final String parameterType) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) attributeMappingMap.get(parameterType);
        final Map<String, Tuple2<Field, DbAttribute>> fieldMap = fieldMapping.getMappedFields();
        final Map<KeyType, Tuple2<String, Field>> pkMapping = fieldMapping.getPrimaryKeyMapping();
        final Set<String> pkFields = pkMapping.values().stream().map(Tuple2::_1).collect(Collectors.toSet());

        return fieldMap.entrySet().stream()
                .map(entry -> Tuple.of(entry.getKey(), entry.getValue()))
                .map(a -> Tuple.of(a._1(), ReflectionUtils.getField(a._2()._1(), input), a._2()._1(), a._2()._2()))
                .filter(a -> !pkFields.contains(a._1()));
    }

    <T> Stream<Tuple4<String, Object, Field, DbAttribute>> getMappedValues(final T input, final Class<T> parameterClass) {
        final DataMapper<T> dataMapper = DataMapperUtils.getDataMapper(parameterClass);

        return dataMapper.getMappedValues(input);
    }

    <T> Stream<Tuple3<String, Field, DbAttribute>> getMappedValues(final String parameterType) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) attributeMappingMap.get(parameterType);
        final Map<String, Tuple2<Field, DbAttribute>> fieldMap = fieldMapping.getMappedFields();
        final Map<KeyType, Tuple2<String, Field>> pkMapping = fieldMapping.getPrimaryKeyMapping();
        final Set<String> pkFields = pkMapping.values().stream().map(Tuple2::_1).collect(Collectors.toSet());

        return fieldMap.entrySet().stream()
                .map(entry -> Tuple.of(entry.getKey(), entry.getValue()))
                .map(a -> Tuple.of(a._1(), a._2()._1(), a._2()._2()))
                .filter(a -> !pkFields.contains(a._1()));
    }

    private <T> Constructor<T> constructor(final Class<T> dataClass) {
        try {
            final Constructor<T> constructor = dataClass.getDeclaredConstructor();

            constructor.setAccessible(true);

            return constructor;
        } catch (final NoSuchMethodException e) {
            throw new UtilsException("UNKNOWN_ERROR", "No empty constructor found for " + dataClass.getName(), e);
        }
    }

    private <T> void setFieldMappings(final Map<String, Tuple2<Field, DbAttribute>> mappedFields,
                                      final Map<KeyType, Tuple2<String, Field>> primaryKeyMapping,
                                      final Field field,
                                      final ConcurrentHashMap<String, GSI.Builder> indexMap,
                                      final Map<String, Tuple2<Field, DbAttribute>> versionAttMap,
                                      final AttributeMapper.Builder<T> builder) {
        final List<Annotation> annotations;

        field.setAccessible(true);
        annotations = Arrays.stream(field.getAnnotations()).filter(a -> (a instanceof DbAttribute ||
                a instanceof Transient ||
                a instanceof HashKey ||
                a instanceof RangeKey ||
                a instanceof SecondaryIndex ||
                a instanceof SecondaryIndices ||
                a instanceof DateUpdated) ||
                a instanceof DateCreated ||
                a instanceof VersionAttribute).collect(Collectors.toList());

        if (!isTransient(annotations)) {
            if (isAnnotatedCorrectly(annotations)) {
                final DbAttribute dbAttribute = !CollectionUtils.isEmpty(annotations) ?
                        (DbAttribute) annotations.stream().filter(a -> (a instanceof DbAttribute)).findAny().orElse(null) : null;
                final DateCreated dateCreated = !CollectionUtils.isEmpty(annotations) ?
                        (DateCreated) annotations.stream().filter(a -> (a instanceof DateCreated)).findAny().orElse(null) : null;
                final DateUpdated dateUpdated = !CollectionUtils.isEmpty(annotations) ?
                        (DateUpdated) annotations.stream().filter(a -> (a instanceof DateUpdated)).findAny().orElse(null) : null;
                final HashKey hashKey = !CollectionUtils.isEmpty(annotations) ?
                        (HashKey) annotations.stream().filter(a -> (a instanceof HashKey)).findAny().orElse(null) : null;
                final RangeKey rangeKey = !CollectionUtils.isEmpty(annotations) ?
                        (RangeKey) annotations.stream().filter(a -> (a instanceof RangeKey)).findAny().orElse(null) : null;
                final SecondaryIndex gsiElement = !CollectionUtils.isEmpty(annotations) ?
                        (SecondaryIndex) annotations.stream().filter(a -> (a instanceof SecondaryIndex)).findAny().orElse(null) : null;
                final SecondaryIndices gsis = !CollectionUtils.isEmpty(annotations) ?
                        (SecondaryIndices) annotations.stream().filter(a -> (a instanceof SecondaryIndices)).findAny().orElse(null) : null;
                final VersionAttribute versionAttribute = !CollectionUtils.isEmpty(annotations) ?
                        (VersionAttribute) annotations.stream().filter(a -> (a instanceof VersionAttribute)).findAny().orElse(null) : null;
                final String fieldName = (dbAttribute != null && !StringUtils.isEmpty(dbAttribute.value())) ? dbAttribute.value() : field.getName();
                final List<SecondaryIndex> gsiList;
                final String fieldNameVal;
                final Class<?> fieldClass = field.getType();

                if (versionAttribute != null && (fieldClass != int.class && fieldClass != Integer.class && fieldClass != Long.class && fieldClass != long.class)) {
                    throw new DbException("VersionedAttribute field can only be of type integer or long");
                }

                if (gsis != null && gsiElement != null) {
                    throw new DbException(MessageFormat.format("Both GSIs and GSI cannot be used for the same field [{0}.{1}]", field.getDeclaringClass(), field.getName()));
                }

                gsiList = gsis != null ? Arrays.asList(gsis.indices()) : Collections.singletonList(gsiElement);

                if(hashKey != null) {
                    primaryKeyMapping.put(KeyType.HASH_KEY, Tuple.of(fieldName, field));
                } else if(rangeKey != null) {
                    primaryKeyMapping.put(KeyType.RANGE_KEY, Tuple.of(fieldName, field));
                }

                gsiList.forEach(gsi -> {
                    if (gsi != null) {
                        final GSI.Builder gsiBuilder = indexMap.computeIfAbsent(gsi.name(), s -> GSI.builder(gsi.name())
                                .projectionType(gsi.projectionType()));
                        final Tuple2<String, Field> keyTuple = Tuple.of(fieldName, field);

                        switch (gsi.type()) {
                            case HASH_KEY:
                                gsiBuilder.hashKeyTuple(keyTuple);
                                break;
                            case RANGE_KEY:
                                gsiBuilder.rangeKeyTuple(keyTuple);
                                break;
                            default:
                                throw new DbException("Unrecognized key type");
                        }
                    }
                });
                fieldNameVal = getFieldName(dbAttribute, dateCreated, dateUpdated, field, builder);

                mappedFields.put(fieldNameVal, Tuple.of(field, dbAttribute));

                if (versionAttribute != null) {
                    versionAttMap.put(fieldNameVal, Tuple.of(field, dbAttribute));
                }

            } else {
                throw new UtilsException("INCORRECT_MODEL_ANNOTATION", "A field can only have one of the following annotation: [DbAttribute, DateCreated, DateUpdated]");
            }
        }
    }

    private <T> String getFieldName(final DbAttribute dbAttribute, final DateCreated dateCreated, final DateUpdated dateUpdated, final Field field, final AttributeMapper.Builder<T> builder) {
        final String fieldName;

        if (dbAttribute != null && !StringUtils.isEmpty(dbAttribute.value())) {
            fieldName = dbAttribute.value();
        } else if (dateCreated != null) {
            fieldName = dateCreated.value();
            builder.dateCreatedField(Tuple.of(dateCreated.value(), field));
        } else if (dateUpdated != null) {
            fieldName = dateUpdated.value();
            builder.dateUpdatedField(Tuple.of(dateUpdated.value(), field));
        } else {
            fieldName = field.getName();
        }

        return fieldName;
    }

    private boolean isTransient(final List<Annotation> annotations) {
        return !CollectionUtils.isEmpty(annotations) && annotations.stream().anyMatch(a -> (a instanceof Transient));
    }

    private boolean isAnnotatedCorrectly(final List<Annotation> annotations) {
        final List<? extends Annotation> annotationList = CollectionUtils.isEmpty(annotations) ?
                annotations.stream().filter(a -> (a instanceof DbAttribute
                        || a instanceof DateCreated
                        || a instanceof DateUpdated)).collect(Collectors.toList()) : Collections.emptyList();

        return CollectionUtils.isEmpty(annotationList) || annotationList.size() == 1;
    }

    public ConcurrentHashMap<String, AttributeMapper<?>> getAttributeMappingMap() {
        return attributeMappingMap;
    }

    static MapperUtils getInstance() {
        return INSTANCE;
    }
}
