package org.awsutils.dynamodb.repositories;



import io.vavr.Tuple2;
import org.awsutils.dynamodb.annotations.ProjectionType;


import java.lang.reflect.Field;

interface GSI {
    String getName();


    /**
     *
     * @return
     */
    Tuple2<String, Field> getHashKeyTuple();

    /**
     *
     * @return
     */
    Tuple2<String, Field> getRangeKeyTuple();

    /**
     *
     * @return
     */
    ProjectionType getProjectionType();

    /**
     *
     * @param indexName
     * @return
     */
    static Builder builder(final String indexName) {
        return GlobalSecondaryIndexImpl.builder(indexName);
    }

    @SuppressWarnings("UnusedReturnValue")
    interface Builder {
        /**
         *
         * @param hashKeyTuple
         * @return
         */
        Builder hashKeyTuple(Tuple2<String, Field> hashKeyTuple);

        /**
         *
         * @param rangeKeyTuple
         * @return
         */
        Builder rangeKeyTuple(Tuple2<String, Field> rangeKeyTuple);

        /**
         *
         * @param projectionType
         * @return
         */
        Builder projectionType(ProjectionType projectionType);

        /**
         *
         * @return
         */
        GSI build();
    }

}
