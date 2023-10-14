package org.awsutils.dynamodb.repositories;


import io.vavr.Tuple2;
import org.awsutils.dynamodb.annotations.KeyType;
import org.awsutils.dynamodb.annotations.ProjectionType;
import org.awsutils.dynamodb.exceptions.DbException;

import java.lang.reflect.Field;
import java.util.Objects;

final class GlobalSecondaryIndexImpl implements GSI {
    private final String name;
    private final Tuple2<String, Field> hashKeyTuple;
    private final Tuple2<String, Field> rangeKeyTuple;
    private final ProjectionType projectionType;

    private GlobalSecondaryIndexImpl(final String name,
                                     final Tuple2<String, Field> hashKeyTuple,
                                     final Tuple2<String, Field> rangeKeyTuple,
                                     final ProjectionType projectionType) {
        this.name = name;
        this.hashKeyTuple = hashKeyTuple;
        this.rangeKeyTuple = rangeKeyTuple;
        this.projectionType = projectionType;
    }

    @Override
    public String getName() {
        return name;
    }


    /**
     *
     * @return
     */
    @Override
    public Tuple2<String, Field> getHashKeyTuple() {
        return hashKeyTuple;
    }

    /**
     *
     * @return
     */
    @Override
    public Tuple2<String, Field> getRangeKeyTuple() {
        return rangeKeyTuple;
    }

    /**
     *
     * @return
     */
    @Override
    public ProjectionType getProjectionType() {
        return projectionType;
    }

    /**
     *
     * @param o
     * @return
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GlobalSecondaryIndexImpl that = (GlobalSecondaryIndexImpl) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(hashKeyTuple, that.hashKeyTuple) &&
                Objects.equals(rangeKeyTuple, that.rangeKeyTuple) &&
                projectionType == that.projectionType;
    }

    /**
     *
     * @return
     */
    @Override
    public int hashCode() {
        return Objects.hash(name, hashKeyTuple, rangeKeyTuple, projectionType);
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return "GlobalSecondaryIndex{" +
                "name='" + name + '\'' +
                ", primaryKeyTuple=" + hashKeyTuple +
                ", sortKeyTuple=" + rangeKeyTuple +
                ", projectionType=" + projectionType +
                '}';
    }

    /**
     *
     * @return
     */
    private GlobalSecondaryIndexImpl validate() {
        if(hashKeyTuple == null) {
            throw new DbException("GSI [" + name + "] does not have a hash key defined");
        }

        if(rangeKeyTuple != null) {
            if(rangeKeyTuple._1().equals(hashKeyTuple._1())) {
                throw new DbException("GSI [" + name + "] - Both hash key and range key have the same field name");
            }
        }

        return this;
    }

    /**
     *
     * @param indexName
     * @return
     */
    static Builder builder(final String indexName) {
        return new BuilderImpl(indexName);
    }

    /**
     *
     */
    private static final class BuilderImpl implements Builder {
        private final String name;
        private Tuple2<String, Field> hashKeyTuple;
        private Tuple2<String, Field> rangeKeyTuple;
        private ProjectionType projectionType;

        private BuilderImpl(final String name) {
            this.name = name;
        }

        /**
         *
         * @param hashKeyTuple
         * @return
         */
        @Override
        public Builder hashKeyTuple(final Tuple2<String, Field> hashKeyTuple) {
            if(this.hashKeyTuple != null) {
                throw new DbException("Cannot have multiple " + KeyType.HASH_KEY + " for the same index [" + name + "]");
            }

            this.hashKeyTuple = hashKeyTuple;
            return this;
        }

        /**
         *
         * @param rangeKeyTuple
         * @return
         */
        @Override
        public Builder rangeKeyTuple(final Tuple2<String, Field> rangeKeyTuple) {
            if(this.rangeKeyTuple != null) {
                throw new DbException("Cannot have multiple " + KeyType.RANGE_KEY + " for the same index [" + name + "]");
            }

            this.rangeKeyTuple = rangeKeyTuple;
            return this;
        }

        @Override
        public Builder projectionType(final ProjectionType projectionType) {
            if(this.projectionType != null && this.projectionType != projectionType) {
                throw new DbException("Cannot define different projection types for Hash Key and Range Key [" + name + "]");
            }
            this.projectionType = projectionType;
            return this;
        }

        @Override
        public GSI build() {
            return new GlobalSecondaryIndexImpl(name, hashKeyTuple, rangeKeyTuple, projectionType == null ?
                    ProjectionType.KEYS_ONLY : projectionType).validate();
        }
    }
}
