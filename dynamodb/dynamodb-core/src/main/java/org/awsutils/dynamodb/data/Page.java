package org.awsutils.dynamodb.data;

public class Page {
    private final int pageSize;
    private final PrimaryKey lastEndKey;

    private Page(final int pageSize, final PrimaryKey lastEndKey) {
        this.pageSize = pageSize;
        this.lastEndKey = lastEndKey;
    }

    public int getPageSize() {
        return pageSize;
    }

    public PrimaryKey getLastEndKey() {
        return lastEndKey;
    }

    public static Builder builder() {
        return new BuilderImpl();
    }

    public interface Builder {
        Builder pageSize(int pageSize);

        Builder lastEndKey(PrimaryKey lastEndKey);

        Page build();
    }

    private static class BuilderImpl implements Builder {
        private int pageSize = 10;
        private PrimaryKey lastEndKey;

        @Override
        public Builder pageSize(int pageSize) {
            this.pageSize = pageSize;

            return this;
        }

        @Override
        public Builder lastEndKey(final PrimaryKey lastEndKey) {
            this.lastEndKey = lastEndKey;

            return this;
        }

        @Override
        public Page build() {
            return new Page(pageSize, lastEndKey);
        }
    }
}
