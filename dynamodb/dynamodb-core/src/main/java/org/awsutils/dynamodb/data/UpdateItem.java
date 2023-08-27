package org.awsutils.dynamodb.data;

import java.util.Map;

@SuppressWarnings({"WeakerAccess", "unused"})
public abstract class UpdateItem {
    private final PrimaryKey primaryKey;
    private final Map<String, Object> updatedValues;


    protected UpdateItem(final PrimaryKey primaryKey, final Map<String, Object> updatedValues) {
        this.primaryKey = primaryKey;
        this.updatedValues = updatedValues;
    }

    public PrimaryKey getPrimaryKey() {
        return this.primaryKey;
    }

    public Map<String, Object> getUpdatedValues() {
        return this.updatedValues;
    }

    public static Builder defaultBuilder() {
        return DefaultUpdateItem.builder();
    }

    public interface Builder {
        Builder primaryKey(PrimaryKey primaryKey);
        Builder updatedValues(Map<String, Object> updatedValues);

        UpdateItem build();
    }

    private static final class DefaultUpdateItem extends UpdateItem {
        private DefaultUpdateItem(final PrimaryKey primaryKey, final Map<String, Object> updatedValues) {
            super(primaryKey, updatedValues);
        }

        private static Builder builder() {
            return new BuilderImpl();
        }

        private static final class BuilderImpl implements Builder {
            private PrimaryKey primaryKey;
            private Map<String, Object> updatedValues;

            @Override
            public Builder primaryKey(final PrimaryKey primaryKey) {
                this.primaryKey = primaryKey;
                return this;
            }

            @Override
            public Builder updatedValues(final Map<String, Object> updatedValues) {
                this.updatedValues = updatedValues;

                return this;
            }

            @Override
            public UpdateItem build() {

                return new DefaultUpdateItem(primaryKey, updatedValues);
            }
        }
    }
}
