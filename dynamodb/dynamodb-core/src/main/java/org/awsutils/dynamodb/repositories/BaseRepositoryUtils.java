package org.awsutils.dynamodb.repositories;

import com.google.common.collect.ImmutableMap;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import org.awsutils.common.util.Utils;
import org.awsutils.dynamodb.annotations.DbAttribute;
import org.awsutils.dynamodb.annotations.DdbRepository;
import org.awsutils.dynamodb.annotations.ProjectionType;
import org.awsutils.dynamodb.data.Page;
import org.awsutils.dynamodb.data.PrimaryKey;
import org.awsutils.dynamodb.data.UpdateItem;
import org.awsutils.dynamodb.exceptions.DbException;
import org.awsutils.dynamodb.exceptions.OptimisticLockFailureException;
import org.awsutils.dynamodb.utils.DbUtils;
import org.awsutils.dynamodb.utils.Expr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ReturnItemCollectionMetrics;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.paginators.QueryPublisher;
import software.amazon.awssdk.services.dynamodb.paginators.ScanPublisher;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
enum BaseRepositoryUtils {
    INSTANCE;

    private final ConcurrentHashMap<String, Class<?>> repoParameterTypeMap = new ConcurrentHashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseRepositoryUtils.class);

    static BaseRepositoryUtils getInstance() {
        return INSTANCE;
    }

    UpdateItemResponse handleUpdateItemException(final PrimaryKey primaryKey,
                                                 final String tableName, final Throwable e) {

        LOGGER.debug(MessageFormat.format("Record with the following primary key [{0}] exists in table [{1}]", primaryKey, tableName), e);
        if (e instanceof CompletionException && e.getCause() instanceof ConditionalCheckFailedException) {
            throw new OptimisticLockFailureException(e.getCause());
        } else if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        } else {
            throw new DbException("UNKNOWN_ERROR" + " - handleUpdateItemException", e);
        }
    }

    <ENTITY_TYPE> Integer setVersion(final ENTITY_TYPE item,
                                     final Tuple2<Field, DbAttribute> versionedAttribute,
                                     final UpdateItemRequest.Builder updateItemRequestBuilder) {

        final Integer version;
        if (versionedAttribute != null) {
            final Number versionNum = (Number) ReflectionUtils.getField(versionedAttribute._1(), item);
            final Class<?> versionFieldType;

            version = versionNum != null ? versionNum.intValue() + 1 : 0;
            versionFieldType = versionedAttribute._1().getType();

            if (versionNum == null) {
                updateItemRequestBuilder.expected(ImmutableMap.of(versionedAttribute._2().value(), ExpectedAttributeValue
                        .builder().exists(false).build()));
            } else {
                final DbAttribute dbAttribute = versionedAttribute._2();
                final String attributeNameValue;

                if(dbAttribute != null) {
                    attributeNameValue = dbAttribute.value();
                } else {
                    attributeNameValue = versionedAttribute._1.getName();
                }

                updateItemRequestBuilder.expected(ImmutableMap.of(attributeNameValue, ExpectedAttributeValue.builder()
                        .value(AttributeValue.builder().n(String.valueOf(versionNum.intValue())).build()).build()));
            }

            if (versionFieldType == Integer.class) {
                ReflectionUtils.setField(versionedAttribute._1(), item, version);
            } else {
                ReflectionUtils.setField(versionedAttribute._1(), item, Long.valueOf(version));
            }
        } else {
            version = null;
        }

        return version;
    }


    /**
     * @param dbRequestFunc  Function to perform DB write actions
     * @param returnItemFunc Function that return items
     * @return Future
     */
    <ENTITY_TYPE> CompletableFuture<List<ENTITY_TYPE>> batchWriteRequest(final Function<DataMapper<ENTITY_TYPE>, Stream<WriteRequest>> dbRequestFunc,
                                                                         final Supplier<List<ENTITY_TYPE>> returnItemFunc,
                                                                         final DataMapper<ENTITY_TYPE> dataMapper) {


        return processBatchWriteRequest(returnItemFunc, ImmutableMap.of(dataMapper.tableName(), dbRequestFunc.apply(dataMapper)
                .collect(Collectors.toList())));
    }

    <ENTITY_TYPE> CompletableFuture<ENTITY_TYPE> updateItem(final PrimaryKey primaryKey,
                                                            final Map<String, Object> updatedValues,
                                                            final Class<ENTITY_TYPE> parameterType,
                                                            final DataMapper<ENTITY_TYPE> dataMapper,
                                                            final ENTITY_TYPE item) {

        final Tuple2<Field, DbAttribute> versionedAttribute = dataMapper.getVersionedAttribute();
        final UpdateItemRequest.Builder updateItemRequestBuilder = UpdateItemRequest.builder();
        final Map<String, Tuple3<String, Field, DbAttribute>> mappedFields = MapperUtils.getInstance().getMappedValues(parameterType.getName())
                .collect(Collectors.toMap(Tuple3::_1, b -> b));
        final Stream<Tuple2<String, AttributeValueUpdate>> mappedValues = updatedValues.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(primaryKey.getHashKeyName()))
                .filter(entry -> !entry.getKey().equals(primaryKey.getRangeKeyName()))
                .peek(a -> {
                    if (mappedFields.get(a.getKey()) != null) {
                        DbUtils.checkForNullFields(mappedFields.get(a.getKey())._3(), a.getValue(), mappedFields.get(a.getKey())._1());
                    }
                })
                .map(a -> {
                    if (mappedFields.get(a.getKey()) != null) {
                        return Tuple.of(a.getKey(), DbUtils.modelToAttributeUpdateValue(mappedFields.get(a.getKey())._2(), a.getValue())
                                .apply(AttributeValueUpdate.builder()).build());
                    } else {
                        return Tuple.of(a.getKey(), AttributeValueUpdate.builder().value(AttributeValue.builder()
                                .s(String.valueOf(a.getValue())).build()).build());
                    }
                });
        final Map<String, AttributeValueUpdate> mappedUpdateValuesTmp = mappedValues.collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
        final Map<String, AttributeValueUpdate> mappedUpdateValues;
        final Integer version = setVersion(item, versionedAttribute, updateItemRequestBuilder);

        if (versionedAttribute != null) {
            mappedUpdateValues = ImmutableMap.<String, AttributeValueUpdate>builder()
                    .putAll(mappedUpdateValuesTmp)
                    .put(versionedAttribute._2().value(), AttributeValueUpdate.builder().value(AttributeValue.builder()
                            .n(String.valueOf(version)).build()).build())
                    .build();
        } else {
            mappedUpdateValues = mappedUpdateValuesTmp;
        }

        return DataMapperUtils.getDynamoDbAsyncClient().updateItem(updateItemRequestBuilder
                        .tableName(dataMapper.tableName())
                        .key(dataMapper.getPrimaryKey(primaryKey))
                        .attributeUpdates(mappedUpdateValues)
                        .returnValues(ReturnValue.ALL_NEW)
                        .build())
                .thenApplyAsync(updateItemResponse -> dataMapper.mapFromAttributeValueToEntity(updateItemResponse.attributes()));
    }

    <ENTITY_TYPE> CompletableFuture<List<ENTITY_TYPE>> findByGlobalSecondaryIndex(final String indexName,
                                                                                  final Object hashKeyValueObj,
                                                                                  final Object rangeKeyValue,
                                                                                  final Function<ENTITY_TYPE, PrimaryKey> primaryKeyFunc,
                                                                                  final Supplier<Class<ENTITY_TYPE>> dataClassFunc,
                                                                                  final Function<List<PrimaryKey>, CompletableFuture<List<ENTITY_TYPE>>> findByPrimaryKeyFunc,
                                                                                  final Expr filterExpression) {

        if (hashKeyValueObj instanceof String) {
            final String hashKeyValue;
            final Tuple2<ProjectionType, CompletableFuture<QueryResponse>> queryResponseTuple;
            final CompletableFuture<List<ENTITY_TYPE>> returnedDataFromDb;
            final CompletableFuture<List<ENTITY_TYPE>> returnData;
            final Class<ENTITY_TYPE> dataClass = dataClassFunc.get();

            if (rangeKeyValue != null && !(rangeKeyValue instanceof String)) {
                throw new DbException("Currently only String types are supported for sortKey Values");
            }

            hashKeyValue = (String) hashKeyValueObj;

            queryResponseTuple = getDataFromIndex(indexName, hashKeyValue, rangeKeyValue, dataClass, filterExpression);

            returnedDataFromDb = queryResponseTuple._2.thenApplyAsync(QueryResponse::items)
                    .thenApplyAsync(list -> list.stream().map(a -> DataMapperUtils.getDataMapper(dataClass).mapFromAttributeValueToEntity(a)).toList());

            if (queryResponseTuple._1() == ProjectionType.ALL) {
                returnData = returnedDataFromDb;
            } else {
                returnData = returnedDataFromDb
                        .thenApplyAsync(x -> x.stream().map(primaryKeyFunc).toList())
                        .thenApplyAsync(x -> !CollectionUtils.isEmpty(x) ? findByPrimaryKeyFunc.apply(x) :
                                CompletableFuture.completedFuture(List.<ENTITY_TYPE>of())).thenCompose(Function.identity());
            }

            return returnData;
        } else {
            throw new DbException("Currently only String types are supported for hashKey Values");
        }
    }

    <ENTITY_TYPE> CompletableFuture<List<ENTITY_TYPE>> findAll(final int pageSize,
                                                               final Supplier<Class<ENTITY_TYPE>> dataClassFunc) {

        return findAll(null, pageSize, dataClassFunc);
    }

    <ENTITY_TYPE> CompletableFuture<List<ENTITY_TYPE>> findAll(final Expr expr,
                                                               final int pageSize,
                                                               final Supplier<Class<ENTITY_TYPE>> dataClassFunc) {

        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(dataClassFunc.get());
        final ScanRequest.Builder scanRequestBuilder = ScanRequest
                .builder()
                .tableName(dataMapper.tableName())
                .limit(pageSize);
        final CompletableFuture<ScanResponse> scanPublisher;

        if (Objects.nonNull(expr)) {
            final Map<String, String> attNameMap = expr.attributeNameMap();
            final Map<String, AttributeValue> attValueMap = expr.attributeValueMap();

            scanRequestBuilder.filterExpression(expr.expression());

            if (!CollectionUtils.isEmpty(attNameMap)) {
                scanRequestBuilder.expressionAttributeNames(attNameMap);
            }

            if (!CollectionUtils.isEmpty(attValueMap)) {
                scanRequestBuilder.expressionAttributeValues(attValueMap);
            }
        }

        scanPublisher = DataMapperUtils.getDynamoDbAsyncClient().scan(scanRequestBuilder.build());

        return scanPublisher.thenApplyAsync(ScanResponse::items).thenApplyAsync(x -> x.stream().map(dataMapper::mapFromAttributeValueToEntity).toList());
    }

    <ENTITY_TYPE> CompletableFuture<Optional<ENTITY_TYPE>> findByPrimaryKey(final PrimaryKey primaryKey,
                                                                            final Supplier<Class<ENTITY_TYPE>> dataClassFunc) {

        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(dataClassFunc.get());
        final GetItemRequest getItemRequest = GetItemRequest.builder()
                .key(dataMapper.getPrimaryKey(primaryKey))
                .tableName(dataMapper.tableName()).build();

        return DataMapperUtils.getDynamoDbAsyncClient().getItem(getItemRequest)
                .thenApplyAsync(itemResponse -> itemResponse.item())
                .thenApplyAsync(item -> item.isEmpty() ? Optional.of(dataMapper.mapFromAttributeValueToEntity(item)) : Optional.empty());
    }

    <ENTITY_TYPE> CompletableFuture<List<ENTITY_TYPE>> findByPrimaryKeys(final List<PrimaryKey> primaryKeys,
                                                                         final Supplier<Class<ENTITY_TYPE>> dataClassFunc) {

        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(dataClassFunc.get());
        final List<PrimaryKey> primaryKeysForQuery = new ArrayList<>(new HashSet<>(primaryKeys)); //Adding list of primary keys to a set before querying to remove duplicates.
        final KeysAndAttributes attributes = KeysAndAttributes.builder().keys(primaryKeysForQuery.stream()
                .map(dataMapper::getPrimaryKey)
                .collect(Collectors.toList())).build();
        final Map<String, KeysAndAttributes> andAttributesMap = new HashMap<>();
        final BatchGetItemRequest request;

        andAttributesMap.put(dataMapper.tableName(), attributes);
        request = BatchGetItemRequest.builder().requestItems(andAttributesMap).build();

        return DataMapperUtils.getDynamoDbAsyncClient().batchGetItem(request)
                .thenApplyAsync(batchGetItemResponse -> batchGetItemResponse.responses().get(dataMapper.tableName()))
                .thenApplyAsync(x -> x.stream().map(dataMapper::mapFromAttributeValueToEntity).toList());
    }

    <ENTITY_TYPE> CompletableFuture<ENTITY_TYPE> saveItem(final ENTITY_TYPE item, final boolean upsert,
                                                          final BiConsumer<ENTITY_TYPE, Map<String, AttributeValue>> ttlAction,
                                                          final DataMapper<ENTITY_TYPE> dataMapper) {

        final PrimaryKey primaryKey = dataMapper.createPKFromItem(item);
        final String tableName = dataMapper.tableName();
        final Map<String, AttributeValue> attributeValues;
        final PutItemRequest putItemRequest;
        final PutItemRequest.Builder builder;
        final Tuple2<Field, DbAttribute> versionedAttribute = dataMapper.getVersionedAttribute();
        final String rangeKeyName = primaryKey.getRangeKeyName();

        builder = PutItemRequest.builder()
                .tableName(dataMapper.tableName());

        if (!upsert) {
            if (versionedAttribute != null) {
                ReflectionUtils.setField(versionedAttribute._1(), item, BigInteger.ZERO.intValue());
            }

            if (!StringUtils.isEmpty(rangeKeyName)) {
                builder.expected(ImmutableMap.of(
                        primaryKey.getHashKeyName(), ExpectedAttributeValue.builder().exists(false).build(),
                        primaryKey.getRangeKeyName(), ExpectedAttributeValue.builder().exists(false).build()));
            } else {
                builder.expected(ImmutableMap.of(
                        primaryKey.getHashKeyName(), ExpectedAttributeValue.builder().exists(false).build()));
            }
        } else {
            if (versionedAttribute != null) {
                final Number versionNum = (Number) ReflectionUtils.getField(versionedAttribute._1(), item);
                final int version;

                if (versionNum == null) {
                    version = BigInteger.ZERO.intValue();

                    builder.expected(ImmutableMap.of(versionedAttribute._2().value(), ExpectedAttributeValue.builder().exists(false).build()));
                } else {
                    version = versionNum.intValue() + 1;

                    builder.expected(ImmutableMap.of(versionedAttribute._2().value(),
                            ExpectedAttributeValue.builder().attributeValueList(AttributeValue.builder().n(String.valueOf(versionNum)).build()).build()));
                }

                ReflectionUtils.setField(versionedAttribute._1(), item, version);
            }
        }

        attributeValues = dataMapper.mapFromEntityToAttributeValue(item);

        ttlAction.accept(item, attributeValues);

        builder.item(attributeValues);

        putItemRequest = builder.build();

        return DataMapperUtils.getDynamoDbAsyncClient().putItem(putItemRequest).thenApplyAsync(putItemResponse -> item)
                .exceptionally(e -> handleCreateItemException(primaryKey, tableName, e));
    }

    <ENTITY_TYPE> ENTITY_TYPE handleCreateItemException(final PrimaryKey primaryKey,
                                                        final String tableName,
                                                        final Throwable e) {

        LOGGER.error(MessageFormat.format("Record with the following primary key [{0}] exists in table [{1}]", primaryKey, tableName), e);

        if (e instanceof CompletionException && e.getCause() instanceof ConditionalCheckFailedException) {
            throw new DbException("RECORD_ALREADY_EXISTS", e);
        } else if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        } else {
            throw new DbException("UNKNOWN_ERROR" + " - handleCreateItemException", e);
        }
    }

    <ENTITY_TYPE> CompletableFuture<ENTITY_TYPE> updateItem(final ENTITY_TYPE item,
                                                            final Function<ENTITY_TYPE, PrimaryKey> primaryKeyFunc,
                                                            final Supplier<Class<ENTITY_TYPE>> dataClassFunc) {

        final PrimaryKey primaryKey = primaryKeyFunc.apply(item);
        final Class<ENTITY_TYPE> dataClass = dataClassFunc.get();
        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(dataClass);
        final Stream<Tuple2<String, AttributeValueUpdate>> mappedValues;
        final Tuple2<Field, DbAttribute> versionedAttribute = dataMapper.getVersionedAttribute();
        final UpdateItemRequest.Builder updateItemRequestBuilder = UpdateItemRequest.builder();

        setVersion(item, versionedAttribute, updateItemRequestBuilder);

        mappedValues = MapperUtils.getInstance().getMappedValues(item, dataClass)
                .peek(a -> DbUtils.checkForNullFields(a._4(), a._2(), a._1()))
                .filter(a -> a._1() != null)
                .map(a -> Tuple.of(a._1(), DbUtils.modelToAttributeUpdateValue(a._3(), a._2()).apply(AttributeValueUpdate.builder()).build()));

        return DataMapperUtils
                .getDynamoDbAsyncClient()
                .updateItem(updateItemRequestBuilder
                        .tableName(dataMapper.tableName())
                        .key(dataMapper.getPrimaryKey(primaryKey))
                        .attributeUpdates(mappedValues.collect(Collectors.toMap(Tuple2::_1, Tuple2::_2)))
                        .returnValues(ReturnValue.ALL_NEW)
                        .build())
                .exceptionally(e -> handleUpdateItemException(primaryKey, dataMapper.tableName(), e))
                .thenApplyAsync(updateItemResponse -> dataMapper.mapFromAttributeValueToEntity(updateItemResponse.attributes()))
                .exceptionallyAsync(throwable -> {
                    throw (throwable instanceof RuntimeException ? (RuntimeException) throwable : new DbException(throwable));
                });
    }

    <ENTITY_TYPE> CompletableFuture<ENTITY_TYPE> updateItem(final PrimaryKey primaryKey,
                                                            final Map<String, Object> updatedValues,
                                                            final Function<PrimaryKey, CompletableFuture<ENTITY_TYPE>> findByPrimaryFunc,
                                                            final Supplier<Class<ENTITY_TYPE>> dataClassFunc) {

        final CompletableFuture<ENTITY_TYPE> itemMono = findByPrimaryFunc.apply(primaryKey);
        final Class<ENTITY_TYPE> parameterType = dataClassFunc.get();
        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(parameterType);

        return itemMono.thenApplyAsync(item -> updateItem(primaryKey, updatedValues, parameterType, dataMapper, item))
                .thenCompose(Function.identity());
    }

    <ENTITY_TYPE> CompletableFuture<List<ENTITY_TYPE>> updateItem(final List<UpdateItem> updateItems, final Supplier<Class<ENTITY_TYPE>> paramTypeFunc,
                                                                  final Function<List<PrimaryKey>, CompletableFuture<List<ENTITY_TYPE>>> findByPrimaryKeysFunc) {

        final Class<ENTITY_TYPE> parameterType = paramTypeFunc.get();
        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(parameterType);
        final CompletableFuture<List<ENTITY_TYPE>> items = findByPrimaryKeysFunc.apply(updateItems.stream().map(UpdateItem::getPrimaryKey).collect(Collectors.toList()));
        final Map<PrimaryKey, UpdateItem> updateItemMap = updateItems.stream().collect(Collectors.toMap(UpdateItem::getPrimaryKey, b -> b));

        final var updatedItemsFutureList = items.thenApplyAsync(x -> x.stream().map(item -> updateItem(dataMapper.createPKFromItem(item),
                updateItemMap.get(dataMapper.createPKFromItem(item)).getUpdatedValues(), parameterType, dataMapper, item)).toList());

        return updatedItemsFutureList.thenApplyAsync(futureList -> CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
                .thenApply(v -> futureList.stream().map(CompletableFuture::join).toList())).thenCompose(Function.identity());
    }

    <ENTITY_TYPE> CompletableFuture<List<ENTITY_TYPE>> batchWrite(final List<ENTITY_TYPE> putItems,
                                                                  final List<ENTITY_TYPE> deleteItems,
                                                                  final Supplier<Class<ENTITY_TYPE>> paramTypeFunc) {

        final Function<DataMapper<ENTITY_TYPE>, Stream<WriteRequest>> putFunc = dataMapper -> putItems.stream().map(item -> WriteRequest.builder()
                .putRequest(PutRequest.builder()
                        .item(dataMapper.mapFromEntityToAttributeValue(item))
                        .build())
                .build());
        final Function<DataMapper<ENTITY_TYPE>, Stream<WriteRequest>> deleteFunc = dataMapper ->
                (deleteItems != null ? deleteItems : Collections.<ENTITY_TYPE>emptyList()).stream().map(item -> WriteRequest.builder()
                        .deleteRequest(DeleteRequest.builder()
                                .key(dataMapper.getPrimaryKey(dataMapper.createPKFromItem(item)))
                                .build())
                        .build());
        final Function<DataMapper<ENTITY_TYPE>, Stream<WriteRequest>> dbRequestFunc = dataMapper -> Stream.concat(putFunc.apply(dataMapper),
                deleteFunc.apply(dataMapper));
        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(paramTypeFunc.get());

        return processBatchWriteRequest(() -> putItems, ImmutableMap.of(dataMapper.tableName(), dbRequestFunc.apply(dataMapper)
                .collect(Collectors.toList())));
    }

    <ENTITY_TYPE> CompletableFuture<List<ENTITY_TYPE>> processBatchWriteRequest(final Supplier<List<ENTITY_TYPE>> returnItemFunc,
                                                                                final Map<String, List<WriteRequest>> requestItems) {

        final BatchWriteItemRequest batchWriteItemRequest;
        final CompletableFuture<BatchWriteItemResponse> response;

        batchWriteItemRequest = BatchWriteItemRequest.builder()
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
                .requestItems(requestItems).build();

        response = DataMapperUtils.getDynamoDbAsyncClient().batchWriteItem(batchWriteItemRequest);

        return response.thenApplyAsync(res -> res.hasUnprocessedItems() && !CollectionUtils.isEmpty(res.unprocessedItems()) ?
                processBatchWriteRequest(returnItemFunc, res.unprocessedItems()) :
                CompletableFuture.completedFuture(returnItemFunc.get())).thenCompose(Function.identity());
    }

    <ENTITY_TYPE> CompletableFuture<ENTITY_TYPE> deleteItem(final ENTITY_TYPE item,
                                                            final Supplier<Class<ENTITY_TYPE>> paramTypeFunc) {

        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(paramTypeFunc.get());
        final PrimaryKey primaryKey = dataMapper.createPKFromItem(item);
        final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
                .tableName(dataMapper.tableName())
                .key(dataMapper.getPrimaryKey(primaryKey)).build();

        return DataMapperUtils.getDynamoDbAsyncClient().deleteItem(deleteRequest).thenApplyAsync(deleteItemResponse -> item);
    }


    @SuppressWarnings("DuplicatedCode")
    <ENTITY_TYPE> Tuple2<ProjectionType, CompletableFuture<QueryResponse>> getDataFromIndex(final String indexName,
                                                                                            final String hashKeyValue,
                                                                                            final Object rangeKeyValue,
                                                                                            final Class<ENTITY_TYPE> dataClass,
                                                                                            final Expr filterExpressions) {

        final AttributeMapper<ENTITY_TYPE> attributeMapper = (AttributeMapper<ENTITY_TYPE>) MapperUtils.getInstance().getAttributeMappingMap()
                .get(dataClass.getName());
        final GSI secondaryIndex = attributeMapper.getGlobalSecondaryIndexMap().get(indexName);

        if (secondaryIndex == null) {
            throw new DbException(MessageFormat.format("Index [{0}] not defined in the data model", indexName));
        } else if (rangeKeyValue != null && secondaryIndex.getRangeKeyTuple() == null) {
            throw new DbException(MessageFormat.format("Sort Key not defined for index[{0}] in the data model", indexName));
        } else {
            final String keyConditionExpression = "#d = :partition_key" + (rangeKeyValue != null ? (" and " + secondaryIndex.getRangeKeyTuple()._1()
                    + " = :sort_key_val") : "");
            final QueryRequest request;
            final CompletableFuture<QueryResponse> queryPublisher;
            final QueryRequest.Builder builder = QueryRequest.builder();
            final Map<String, String> nameMap = new HashMap<>(Map.of("#d", secondaryIndex.getHashKeyTuple()._1()));
            final Map<String, AttributeValue> attributeValueMap = new HashMap<>();

            if (rangeKeyValue != null) {
                attributeValueMap.put(":sort_key_val", AttributeValue.builder().s(String.valueOf(rangeKeyValue)).build());
            }

            attributeValueMap.put(":partition_key", AttributeValue.builder().s(hashKeyValue).build());

            builder.tableName(attributeMapper.getTableName());
            builder.indexName(secondaryIndex.getName());

            setFilterExpression(filterExpressions, builder, nameMap, attributeValueMap);

            builder.keyConditionExpression(keyConditionExpression);
            builder.expressionAttributeNames(nameMap);
            builder.expressionAttributeValues(attributeValueMap);

            request = builder.build();

            queryPublisher = DataMapperUtils.getDynamoDbAsyncClient().query(request);

            return Tuple.of(secondaryIndex.getProjectionType(), queryPublisher);
        }
    }

    void setFilterExpression(final Expr expr,
                             final QueryRequest.Builder builder,
                             final Map<String, String> nameMap,
                             final Map<String, AttributeValue> attributeValueMap) {

        if (expr != null) {
            final Map<String, String> attNameMap = expr.attributeNameMap();
            final Map<String, AttributeValue> attValueMap = expr.attributeValueMap();

            builder.filterExpression(expr.expression());
            if (!CollectionUtils.isEmpty(attNameMap)) {
                nameMap.putAll(attNameMap);
            }

            if (!CollectionUtils.isEmpty(attValueMap)) {
                attributeValueMap.putAll(attValueMap);
            }
        }
    }

    <ENTITY_TYPE> Class<ENTITY_TYPE> getRepoParameterType(
            final DynamoDbRepository<ENTITY_TYPE> baseRepository) {

        return (Class<ENTITY_TYPE>) repoParameterTypeMap.computeIfAbsent(baseRepository.getClass().getName(), s -> {
            final DdbRepository annotation = baseRepository.getClass().getAnnotation(DdbRepository.class);

            if (annotation != null) {
                return annotation.entityClass();
            } else {
                throw new DbException("Annotation not defined in Repository implementation.");
            }
        });
    }

    <ENTITY_TYPE> CompletableFuture<List<ENTITY_TYPE>> findByHashKeyAndRangeKeyStartsWithPagination(final String hashKey,
                                                                                 final Object hashKeyValueObj,
                                                                                 final String rangeKey,
                                                                                 final String rangeKeyValue,
                                                                                 final Page page,
                                                                                 @Nullable final String indexName,
                                                                                 final Class<ENTITY_TYPE> dataClass,
                                                                                 @Nullable final Expr expr) {

        if ((hashKeyValueObj instanceof String) || hashKeyValueObj instanceof Number) {
            final QueryRequest request;
            final CompletableFuture<QueryResponse> queryResponse;
            final Map<String, String> nameMap = new HashMap<>();
            final Map<String, AttributeValue> attributeValueMap = new HashMap<>();
            final String hashAlias = "#a";
            final String keyConditionExpression;
            final QueryRequest.Builder builder = QueryRequest.builder();
            final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(dataClass);


            if (!StringUtils.isEmpty(rangeKey) && !StringUtils.isEmpty(rangeKeyValue)) {
                keyConditionExpression = MessageFormat.format("{0} = :{1} and begins_with({2}, :sortKeyVal)", hashAlias, hashKey, rangeKey);
            } else {
                keyConditionExpression = hashAlias + " = :" + hashKey;
            }

            nameMap.put(hashAlias, hashKey);

            if (hashKeyValueObj instanceof String) {
                attributeValueMap.put(MessageFormat.format(":{0}", hashKey), AttributeValue.builder().s((String) hashKeyValueObj).build());
            } else {
                attributeValueMap.put(MessageFormat.format(":{0}", hashKey), AttributeValue.builder().n(Utils.getUnformattedNumber((Number) hashKeyValueObj)).build());
            }

            if (!StringUtils.isEmpty(rangeKey) && !StringUtils.isEmpty(rangeKeyValue)) {
                attributeValueMap.put(":sortKeyVal", AttributeValue.builder().s(rangeKeyValue).build());
            }

            if (!StringUtils.isEmpty(indexName)) {
                builder.indexName(indexName);
            }

            setFilterExpression(expr, builder, nameMap, attributeValueMap);

            if (page != null) {
                builder.limit(page.getPageSize());

                if (page.getLastEndKey() != null) {
                    final String lastEndKeyVal = (String) page.getLastEndKey().getRangeKeyValue();

                    if (StringUtils.isEmpty(rangeKeyValue) || lastEndKeyVal.startsWith(rangeKeyValue)) {
                        builder.exclusiveStartKey(dataMapper.getPrimaryKey(page.getLastEndKey()));
                    } else {
                         throw new DbException("INVALID_RANGE_KEY_VALUE");
                    }
                }
            }

            request = builder
                    .tableName(dataMapper.tableName())
                    .keyConditionExpression(keyConditionExpression)
                    .expressionAttributeNames(nameMap)
                    .expressionAttributeValues(attributeValueMap)
                    .build();

            queryResponse = DataMapperUtils.getDynamoDbAsyncClient().query(request);

            return queryResponse.thenApplyAsync(QueryResponse::items).thenApplyAsync(list -> list.stream().map(dataMapper::mapFromAttributeValueToEntity).toList());
        } else {
            throw new DbException("Currently only String/Number types are supported for hashKey Values");
        }
    }
}
