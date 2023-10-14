package org.awsutils.dynamodb.data;

import java.util.Objects;

@SuppressWarnings({"unused"})
public final class PrimaryKey {
    private final String hashKeyName;
    private final Object hashKeyValue;
    private final String rangeKeyName;
    private final Object rangeKeyValue;

    private PrimaryKey(final String hashKeyName, final Object hashKeyValue, final String rangeKeyName, final Object rangeKeyValue) {
        this.hashKeyName = hashKeyName;
        this.hashKeyValue = hashKeyValue;
        this.rangeKeyName = rangeKeyName;
        this.rangeKeyValue = rangeKeyValue;
    }

    public static Builder builder() {
        return new PrimaryKeyBuilder();
    }

    public static Builder builder(final String hashKeyName, final Object hashKeyValue) {
        return new PrimaryKeyBuilder().hashKeyName(hashKeyName).hashKeyValue(hashKeyValue);
    }

    /**
     * @return Returns Hash Key
     */
    public String getHashKeyName() {
        return hashKeyName;
    }

    /**
     * @return hash key value
     */
    public Object getHashKeyValue() {
        return hashKeyValue;
    }

    /**
     * @return Range Key name
     */
    public String getRangeKeyName() {
        return this.rangeKeyName;
    }

    /**
     *
     * @return Range Key Value
     */
    public Object getRangeKeyValue() {
        return this.rangeKeyValue;
    }

    public Builder toBuilder() {
        return builder(hashKeyName, hashKeyValue).rangeKeyValue(hashKeyValue).rangeKeyValue(rangeKeyValue);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final PrimaryKey that = (PrimaryKey) o;
        return Objects.equals(hashKeyName, that.hashKeyName) &&
                Objects.equals(hashKeyValue, that.hashKeyValue) &&
                Objects.equals(rangeKeyName, that.rangeKeyName) &&
                Objects.equals(rangeKeyValue, that.rangeKeyValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashKeyName, hashKeyValue, rangeKeyName, rangeKeyValue);
    }

    @Override
    public String toString() {
        return "[" +
                "hashKeyName='" + hashKeyName + '\'' +
                ", hashKeyValue=" + hashKeyValue +
                ", rangeKeyName='" + rangeKeyName + '\'' +
                ", rangeKeyValue=" + rangeKeyValue +
                ']';
    }

    public interface Builder {

        Builder hashKeyName(String hashKeyName);

        Builder hashKeyValue(Object hashKeyValue);

        Builder rangeKeyName(String rangeKeyName);

        Builder rangeKeyValue(Object rangeKeyValue);

        PrimaryKey build();
    }

    private static class PrimaryKeyBuilder implements Builder {
        private String hashKeyName;
        private Object hashKeyValue;
        private String rangeName;
        private Object rangeKeyValue;

        PrimaryKeyBuilder() {
        }

        /**
         *
         * @param hashKeyName Hash Key Name
         * @return Builder
         */
        @Override
        public Builder hashKeyName(final String hashKeyName) {
            this.hashKeyName = hashKeyName;
            return this;
        }

        /**
         *
         * @param hashKeyValue Hash Key Value
         * @return Builder
         */
        @Override
        public Builder hashKeyValue(final Object hashKeyValue) {
            this.hashKeyValue = hashKeyValue;
            return this;
        }

        /**
         *
         * @param rangeKeyName Range Key Name
         * @return Builder
         */
        @Override
        public Builder rangeKeyName(final String rangeKeyName) {
            this.rangeName = rangeKeyName;
            return this;
        }

        /**
         *
         * @param rangeKeyValue Range Key Value
         * @return Builder
         */
        @Override
        public Builder rangeKeyValue(final Object rangeKeyValue) {
            this.rangeKeyValue = rangeKeyValue;
            return this;
        }

        /**
         *
         * @return Primary Key Object
         */
        @Override
        public PrimaryKey build() {
            return new PrimaryKey(hashKeyName, hashKeyValue, rangeName, rangeKeyValue);
        }

        @Override
        public String toString() {
            return "PrimaryKeyBuilder{" +
                    "hashKeyName='" + hashKeyName + '\'' +
                    ", hashKeyValue='" + hashKeyValue + '\'' +
                    ", rangeName='" + rangeName + '\'' +
                    ", rangeKeyValue='" + rangeKeyValue + '\'' +
                    '}';
        }
    }
}
