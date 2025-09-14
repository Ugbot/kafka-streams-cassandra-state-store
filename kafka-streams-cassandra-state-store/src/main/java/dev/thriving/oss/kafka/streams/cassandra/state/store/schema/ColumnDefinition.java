package dev.thriving.oss.kafka.streams.cassandra.state.store.schema;

/**
 * Represents a column definition in a Cassandra table schema.
 * Defines the column name, CQL type, and whether it's part of the primary key.
 *
 * @param name the column name
 * @param cqlType the CQL data type
 * @param isPrimaryKey whether this column is part of the primary key
 * @param isPartitionKey whether this column is a partition key
 * @param isClusteringKey whether this column is a clustering key
 */
public record ColumnDefinition(
        String name,
        String cqlType,
        boolean isPrimaryKey,
        boolean isPartitionKey,
        boolean isClusteringKey
) {

    public ColumnDefinition {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Column name cannot be null or blank");
        }
        if (cqlType == null || cqlType.isBlank()) {
            throw new IllegalArgumentException("CQL type cannot be null or blank");
        }

        // Validation
        if (isPartitionKey && isClusteringKey) {
            throw new IllegalArgumentException("Column cannot be both partition key and clustering key");
        }
        if ((isPartitionKey || isClusteringKey) && !isPrimaryKey) {
            throw new IllegalArgumentException("Partition key and clustering key columns must be primary key columns");
        }
    }

    /**
     * Creates a builder for constructing ColumnDefinition instances.
     */
    public static Builder builder(String name, String cqlType) {
        return new Builder(name, cqlType);
    }

    /**
     * Builder for creating ColumnDefinition instances with a fluent API.
     */
    public static final class Builder {
        private final String name;
        private final String cqlType;
        private boolean isPrimaryKey = false;
        private boolean isPartitionKey = false;
        private boolean isClusteringKey = false;

        private Builder(String name, String cqlType) {
            this.name = name;
            this.cqlType = cqlType;
        }

        public Builder primaryKey() {
            this.isPrimaryKey = true;
            return this;
        }

        public Builder partitionKey() {
            this.isPartitionKey = true;
            this.isPrimaryKey = true;
            return this;
        }

        public Builder clusteringKey() {
            this.isClusteringKey = true;
            this.isPrimaryKey = true;
            return this;
        }

        public ColumnDefinition build() {
            return new ColumnDefinition(name, cqlType, isPrimaryKey, isPartitionKey, isClusteringKey);
        }
    }
}
