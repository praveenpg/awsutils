package org.awsutils.dynamodb.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.awsutils.common.exceptions.UtilsException;
import org.awsutils.common.util.Utils;
import org.awsutils.dynamodb.annotations.DbAttribute;
import org.awsutils.dynamodb.exceptions.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SuppressWarnings({"unused", "unchecked", "rawtypes", "CollectionAddAllCanBeReplacedWithConstructor", "Convert2MethodRef", "RedundantSuppression"})
public final class DbUtils {
    private static final TimeZone SERVER_TIME_ZONE = TimeZone.getTimeZone("America/Los_Angeles");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private static final Logger LOGGER = LoggerFactory.getLogger(DbUtils.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        final SimpleModule module = new SimpleModule();

        module.addSerializer(AttributeValue.class, new AttributeValueSerializer());
        OBJECT_MAPPER.registerModule(module);
    }


    private DbUtils() {
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    public static long getItemExpirationByTTLType(final String ttlType, final long ttlValue) {
        return switch (ttlType) {
            case "days" -> ZonedDateTime.now(ZoneOffset.UTC).plusDays(ttlValue).toEpochSecond();
            case "hours" -> ZonedDateTime.now(ZoneOffset.UTC).plusHours(ttlValue).toEpochSecond();
            case "minutes" -> ZonedDateTime.now(ZoneOffset.UTC).plusMinutes(ttlValue).toEpochSecond();
            default -> ZonedDateTime.now(ZoneOffset.UTC).plusHours(ttlValue).toEpochSecond();
        };
    }

    public static Function<AttributeValue, Object> attributeToModel(final Field field) {
        try {
            final Function<AttributeValue, Object> func1;

            if (field.getType().isEnum()) {
                final Method valueOf = field.getType().getMethod("valueOf", String.class);

                func1 = a -> ReflectionUtils.invokeMethod(valueOf, null, a.s());
            } else if (field.getType() == Long.class || field.getType() == long.class) {

                func1 = a -> a.n() != null ? Long.valueOf(a.n()) : null;
            } else if (field.getType() == Integer.class || field.getType() == int.class) {

                func1 = a -> Integer.parseInt(a.n());
            } else if (field.getType() == String.class) {

                func1 = AttributeValue::s;
            } else if (field.getType() == Double.class || field.getType() == double.class) {

                func1 = a -> Double.parseDouble(a.n());
            } else if (field.getType() == boolean.class || field.getType() == Boolean.class) {

                func1 = AttributeValue::bool;
            } else if (field.getType() == Date.class) {
                func1 = attributeValue -> Date.from(ZonedDateTime.parse(attributeValue.s(), DATE_TIME_FORMATTER).toInstant());
            } else if (Set.class.isAssignableFrom(field.getType())) {
                func1 = a -> attributeToCollection(field, a, () -> new HashSet<>());
            } else if (List.class.isAssignableFrom(field.getType())) {
                func1 = a -> attributeToCollection(field, a, () -> new ArrayList<>());
            } else if (field.getType() == Map.class) {
                func1 = DbUtils::toMappedObject;
            } else if (field.getType().isArray()) {
                func1 = a -> attributeToArray(field, a);
            } else {
                func1 = a -> toMappedObject(field.getType(), a);
            }

            return func1;
        } catch (final NoSuchMethodException e) {
            throw new UtilsException("UNKNOWN_ERROR", e);
        }
    }

    public static Object attributeToArray(final Field field, final AttributeValue attributeValue) {
        final Class arrayType = field.getType();
        final Class fieldType = field.getType().getComponentType();

        if (attributeValue.hasSs()) {
            final List<String> set = new ArrayList<>();

            set.addAll(attributeValue.ss());
            Object arr = Array.newInstance(fieldType, set.size());

            IntStream.range(0, set.size()).forEach(i -> Array.set(arr, i, set.get(i)));
        } else if (attributeValue.hasNs()) {
            final List<String> set = new ArrayList<>();

            set.addAll(createValueCollection(attributeValue.ns(), attributeValue, s -> (Set) s.collect(Collectors.toSet()), fieldType));

            Object arr = Array.newInstance(fieldType, set.size());

            IntStream.range(0, set.size()).forEach(i -> Array.set(arr, i, set.get(i)));

            return arr;
        } else if (attributeValue.hasL()) {
            return toArrayObject(arrayType, attributeValue);
        } else {
            throw new DbException("UNSUPPORTED_ARRAY_TYPE");
        }

        return null;
    }

    public static Object attributeToCollection(final Field field, final AttributeValue attributeValue, final Supplier<Collection> collectionFunc) {
        final Class<?> fieldType = field.getType();
        final Collection attToCollection;

        if (attributeValue.hasSs()) {
            final Collection<String> coll = !fieldType.isInterface() ? (Collection<String>) Utils.constructObject(fieldType) : collectionFunc.get();

            coll.addAll(attributeValue.ss());
            attToCollection = coll;
        } else if (attributeValue.hasNs()) {
            final Collection<String> coll = !fieldType.isInterface() ? (Collection<String>) Utils.constructObject(fieldType) : collectionFunc.get();

            coll.addAll(createValueSet(field, attributeValue.ns(), attributeValue));
            attToCollection = coll;
        } else if (attributeValue.hasL()) {
            final Collection<?> coll = !fieldType.isInterface() ? (Collection<String>) Utils.constructObject(fieldType) : collectionFunc.get();

            coll.addAll((Collection) toListObject(attributeValue, getParameterizedType(field)));
            attToCollection = coll;
        } else {
            throw new DbException("UNSUPPORTED_TYPE_COLLECTION");
        }

        return attToCollection;
    }

    private static Class<?> getParameterizedType(final Field field) {
        final ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
        return (Class<?>) parameterizedType.getActualTypeArguments()[0];
    }

    private static Collection createValueCollection(final Field field, final List<String> attributeValueList, final AttributeValue attributeValue, Function<Stream, Collection> collectionFunc) {
        return createValueCollection(attributeValueList, attributeValue, collectionFunc, getParameterizedType(field));
    }

    private static Collection createValueCollection(final List<String> attributeValueList, final AttributeValue attributeValue, final Function<Stream, Collection> collectionFunc, final Class<?> paramType) {
        if (attributeValue.hasSs() && (paramType != String.class)) {
            throw new DbException("List of string cannot be assigned to list of " + paramType.getSimpleName() + " defined in entity");
        } else if (attributeValue.hasNs() && (paramType != (String.class))) {
            throw new DbException("List of numbers cannot be assigned to list of " + paramType.getSimpleName() + " defined in entity");
        }

        if (Number.class.isAssignableFrom(paramType)) {
            return collectionFunc.apply(attributeValueList.stream().map(a -> convertToNumber(paramType, a)));
        } else {
            throw new DbException("Only Set of String or numbers supported");
        }
    }

    private static Set createValueSet(final Field field, final List<String> attributeValueList, final AttributeValue attributeValue) {

        return (Set) createValueCollection(field, attributeValueList, attributeValue, s -> (Set) s.collect(Collectors.toSet()));
    }

    private static List createValueList(final Field field, final List<String> attributeValueList, final AttributeValue attributeValue) {
        return (List) createValueCollection(field, attributeValueList, attributeValue, s -> (List) s.collect(Collectors.toList()));
    }

    private static Number convertToNumber(final Class numberClass, final String value) {
        if (numberClass == Integer.class) {
            return Integer.parseInt(value);
        } else if (numberClass == Long.class) {
            return Long.parseLong(value);
        } else if (numberClass == Double.class) {
            return Double.parseDouble(value);
        } else if (numberClass == BigInteger.class) {
            return BigInteger.valueOf(Long.parseLong(value));
        } else if (numberClass == BigDecimal.class) {
            return BigDecimal.valueOf(Double.parseDouble(value));
        } else if (numberClass == AtomicInteger.class) {
            return new AtomicInteger(Integer.parseInt(value));
        } else if (numberClass == AtomicLong.class) {
            return new AtomicLong(Long.parseLong(value));
        } else {
            throw new IllegalArgumentException("Invalid Number type or number type not supported");
        }
    }

    private static String convertNumberToString(final Number value) {
        final Class<? extends Number> numberClass = value.getClass();
        final NumberFormat numberFormat = NumberFormat.getInstance();

        numberFormat.setGroupingUsed(false);

        if (numberClass == Integer.class || numberClass == Long.class || numberClass == BigInteger.class || numberClass == AtomicInteger.class || numberClass == AtomicLong.class) {
            numberFormat.setMaximumFractionDigits(BigInteger.ZERO.intValue());
            numberFormat.setMinimumFractionDigits(BigInteger.ZERO.intValue());

            return numberFormat.format(value.longValue());
        } else if (numberClass == Double.class) {
            numberFormat.setMaximumFractionDigits(BigInteger.ZERO.intValue());
            numberFormat.setMinimumFractionDigits(BigInteger.TEN.intValue());

            return numberFormat.format(value);
        } else if (numberClass == BigDecimal.class) {
            numberFormat.setMaximumFractionDigits(BigInteger.ZERO.intValue());
            numberFormat.setMinimumFractionDigits(BigInteger.TEN.intValue());

            return numberFormat.format(value.doubleValue());
        } else {
            throw new IllegalArgumentException("Invalid Number type or number type not supported");
        }
    }

    private static Map<String, ?> toMappedObject(final AttributeValue attributeValue) {
        return toMappedObject(Map.class, attributeValue);
    }

    private static <T> T toMappedObject(Class<T> mappedClass, final AttributeValue attributeValue) {
        try {
            if (attributeValue.hasM()) {
                return Utils.constructFromJson(mappedClass, OBJECT_MAPPER.writeValueAsString(attributeValue));
            } else {
                throw new DbException("UNKNOWN_TYPE_TO_MAPPED");
            }
        } catch (Exception e) {
            throw new DbException(e.getMessage(), e);
        }
    }

    private static List<?> toListObject(final AttributeValue attributeValue, final Class paramType) {
        try {
            if (attributeValue.hasL()) {
                return Utils.constructListFromJson(paramType, OBJECT_MAPPER.writeValueAsString(attributeValue));
            } else {
                throw new DbException("UNKNOWN_LIST_TYPE");
            }
        } catch (JsonProcessingException e) {
            throw new DbException(e.getMessage(), e);
        }
    }

    private static Object toArrayObject(final Class<?> arrayType, final AttributeValue attributeValue) {
        try {
            if (attributeValue.hasL()) {
                Object ret = Utils.constructFromJson(arrayType, OBJECT_MAPPER.writeValueAsString(attributeValue));

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("return object: " + ret);
                }

                return ret;
            } else {
                throw new DbException("UNKNOWN_ARRAY_TYPE");
            }
        } catch (JsonProcessingException e) {
            throw new DbException(e.getMessage(), e);
        }
    }

    public static Object serializeValue(final Field field, final Object value) {
        try {
            final Object objValue;

            if (field.getType().isEnum()) {
                final Method name = field.getType().getMethod("name");

                objValue = ReflectionUtils.invokeMethod(name, value);
            } else {
                objValue = value;
            }

            return objValue;
        } catch (final NoSuchMethodException e) {
            throw new UtilsException("Error while converting enum to string representation", e);
        }
    }

    public static Function<AttributeValueUpdate.Builder, AttributeValueUpdate.Builder> modelToAttributeUpdateValue(final Field field, final Object value) {
        final Function<AttributeValueUpdate.Builder, AttributeValueUpdate.Builder> action;
        final AttributeValue.Builder attValueBuilder = AttributeValue.builder();
        final Class<?> fieldType = field.getType();

        if (value == null) {
            action = a -> a.action(AttributeAction.DELETE);
        } else if (value instanceof Collection && CollectionUtils.isEmpty((Collection) value)) {
            action = a -> a.action(AttributeAction.DELETE);
        } else if (fieldType.isArray()) {
            action = modelToAttributeUpdateValue(field, Utils.convertArrayToList(fieldType.getComponentType(), value));
        } else {
            action = a -> a.value(modelToAttributeValue(field, value).apply(attValueBuilder).build());
        }

        return action;
    }

    public static void checkForNullFields(final DbAttribute dbAttribute, final Object value, final String fieldName) {
        if (dbAttribute != null && !dbAttribute.nullable() && value == null) {
            throw new DbException(fieldName + " cannot be null...");
        }
    }

    public static Function<AttributeValue.Builder, AttributeValue.Builder> modelToAttributeValue(final Field field, final Object value) {
        final Class<?> fieldType = field.getType();
        final Function<Collection, Class<?>> paramTypeFuncForList = a -> (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];

        return modelToAttributeValue(value, fieldType, paramTypeFuncForList);
    }

    public static Function<AttributeValue.Builder, AttributeValue.Builder> modelToAttributeValue(final Object value,
                                                                                              final Class<?> fieldType,
                                                                                              final Function<Collection, Class<?>> paramTypeFuncForList) {

        try {
            final Function<AttributeValue.Builder, AttributeValue.Builder> action;

            if (fieldType.isEnum()) {
                final Method name = fieldType.getMethod("name");

                action = a -> a.s((String) ReflectionUtils.invokeMethod(name, value));
            } else if (fieldType == String.class) {

                action = a -> a.s(String.valueOf(value));
            } else if (fieldType == Long.class || fieldType == long.class) {

                action = a -> a.n(String.valueOf(value));
            } else if (fieldType == Integer.class || fieldType == int.class) {

                action = a -> a.n(String.valueOf(value));
            } else if (fieldType == Double.class || fieldType == double.class) {
                action = a -> a.n(String.valueOf(value));
            } else if (fieldType == Number.class) {
                action = a -> a.n(String.valueOf(value));
            } else if (fieldType == boolean.class || fieldType == Boolean.class) {
                action = a -> a.bool((Boolean) value);
            } else if (fieldType == Date.class) {
                action = a -> a.s(((Date) value).toInstant().atZone(SERVER_TIME_ZONE.toZoneId()).format(DATE_TIME_FORMATTER));
            } else if (Collection.class.isAssignableFrom(fieldType)) {
                action = getAttributeValueFromList((Collection) value, paramTypeFuncForList);
            } else if (Map.class.isAssignableFrom(fieldType)) {
                action = a -> a.m(getAttributeValueFromMap((Map<String, Object>) value));
            } else if (fieldType.isArray()) {
                final List listVals = Utils.convertArrayToList(fieldType.getComponentType(), value);

                action = getAttributeValueFromList(listVals, l -> fieldType.getComponentType());
            } else {
                action = a -> a.m(getAttributeValueFromMap(Utils.constructFromJson(Map.class, Utils.constructJson(value))));
            }

            return action;
        } catch (final NoSuchMethodException e) {
            throw new UtilsException("UNKNOWN_ERROR", e);
        }
    }

    private static Function<AttributeValue.Builder, AttributeValue.Builder> getAttributeValueFromList(final Collection values,
                                                                                                   final Function<Collection, Class<?>> paramTypeFunc) {
        final Class<?> paramType = paramTypeFunc.apply(values);
        final Function<AttributeValue.Builder, AttributeValue.Builder> action;
        if (paramType == String.class) {
            action = a -> a.ss(((Collection<String>) values).toArray(new String[]{}));
        } else if (Number.class.isAssignableFrom(paramType)) {
            action = a -> a.ns(((Collection<Number>) values).stream().map(DbUtils::convertNumberToString).toList().toArray(new String[]{}));
        } else {
            final Stream<AttributeValue> attList = (values).stream()
                    .map(b -> AttributeValue.builder().m(getAttributeValueFromMap(Utils.constructFromJson(Map.class, Utils.constructJson(b)))).build());

            action = a -> a.l(attList.collect(Collectors.toList()));
        }
        return action;
    }

    public static Map<String, AttributeValue> getAttributeValueFromMap(final Map<String, Object> valuesMap) {
        final Map<String, AttributeValue> attributeValueMap = valuesMap.
                entrySet()
                .stream()
                .filter(entry -> entry.getValue() != null)
                .map(entry -> getStringAttributeValueFromMapEntry(entry))
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("attributeValueMap: " + attributeValueMap);
        }

        return attributeValueMap;
    }

    private static Tuple2<String, AttributeValue> getStringAttributeValueFromMapEntry(final Map.Entry<String, Object> entry) {
        final AttributeValue.Builder builder = AttributeValue.builder();
        final Object value = entry.getValue();

        return Tuple.of(entry.getKey(), modelToAttributeValue(value, value.getClass(), list -> {
            if (CollectionUtils.isEmpty(list)) {
                return String.class;
            } else {
                return list.iterator().next().getClass();
            }
        }).apply(builder).build());
    }


    public static List<Tuple2<String, AttributeValueUpdate>> getUpdatedTime(final Map<String, Object> updatedValues,
                                                                           final Supplier<Tuple2<String, Field>> dateUpdateFieldTupleFunc) {
        final Tuple2<String, Field> dateUpdateFieldTuple = dateUpdateFieldTupleFunc.get();

        if (dateUpdateFieldTuple != null && !updatedValues.containsKey(dateUpdateFieldTuple._1())) {
            final Tuple2<String, AttributeValueUpdate> tuple = Tuple.of(dateUpdateFieldTuple._1(),
                    AttributeValueUpdate.builder().value(AttributeValue.builder()
                            .s(ZonedDateTime.now(SERVER_TIME_ZONE.toZoneId())
                                    .format(DATE_TIME_FORMATTER)).build()).build());

            return Collections.singletonList(tuple);
        } else {
            return Collections.emptyList();
        }
    }

    public static String formatDynamoDbDate(final Date date) {
        return date.toInstant().atZone(SERVER_TIME_ZONE.toZoneId()).format(DATE_TIME_FORMATTER);
    }

    public static Date parseDynamoDbDate(final String formattedDate) {
        return Date.from(ZonedDateTime.parse(formattedDate, DATE_TIME_FORMATTER).toInstant());
    }

    public static Map<String, Object> convertAttributeValueToMap(final AttributeValue attributeValue) {
        final Map<String, Object> map = new HashMap<>();
        final Map<String, AttributeValue> attMap = attributeValue.m();

        attMap.entrySet().stream().filter(entry -> entry.getValue() != null)
                .forEach(entry -> addToAttributeValueMap(entry, map));

        return map;
    }

    private static void addToAttributeValueMap(Map.Entry<String, AttributeValue> entry, Map<String, Object> map) {
        final AttributeValue attValue = entry.getValue();
        final String key = entry.getKey();
        final Object value;

        value = getValueFromAttributeValue(attValue);

        map.put(key, value);
    }

    private static Object getValueFromAttributeValue(final AttributeValue attValue) {
        final Object value;
        if (attValue.hasM()) {
            value = convertAttributeValueToMap(attValue);
        } else if (attValue.hasL()) {
            value = convertAttributeValueToList(attValue);
        } else if (attValue.hasSs()) {
            value = attValue.ss();
        } else if (attValue.hasNs()) {
            value = attValue.ss();
        } else if (attValue.s() != null) {
            value = attValue.s();
        } else if (attValue.n() != null) {
            value = attValue.n();
        } else {
            throw new IllegalStateException("Unsupported type");
        }
        return value;
    }

    public static List<?> convertAttributeValueToList(final AttributeValue attributeValue) {
        return attributeValue.l()
                .stream()
                .map(DbUtils::getValueFromAttributeValue)
                .collect(Collectors.toList());
    }
}