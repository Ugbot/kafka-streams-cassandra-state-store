package dev.thriving.oss.kafka.streams.cassandra.state.store.schema;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Defines a complete Cassandra table schema for a Kafka Streams state store.
 * Supports typed columns instead of generic BLOB types for better performance and type safety.
 */
public final class CassandraSchema {

    private final String tableName;
    private final List<ColumnDefinition> columns;
    private final Map<String, ColumnDefinition> columnMap;

    private CassandraSchema(Builder builder) {
        this.tableName = Objects.requireNonNull(builder.tableName, "Table name cannot be null");
        this.columns = List.copyOf(builder.columns);
        this.columnMap = columns.stream()
                .collect(Collectors.toMap(ColumnDefinition::name, col -> col));

        validateSchema();
    }

    private void validateSchema() {
        // Check for duplicate column names
        Set<String> columnNames = new HashSet<>();
        for (ColumnDefinition column : columns) {
            if (!columnNames.add(column.name())) {
                throw new IllegalArgumentException("Duplicate column name: " + column.name());
            }
        }

        // Check primary key structure
        List<ColumnDefinition> primaryKeys = getPrimaryKeyColumns();
        if (primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("Schema must have at least one primary key column");
        }

        List<ColumnDefinition> partitionKeys = getPartitionKeyColumns();
        if (partitionKeys.isEmpty()) {
            throw new IllegalArgumentException("Schema must have at least one partition key column");
        }

        // Ensure standard columns exist for Kafka Streams compatibility
        if (!hasColumn("time")) {
            throw new IllegalArgumentException("Schema must include a 'time' column for timestamp tracking");
        }

        ColumnDefinition timeColumn = getColumn("time");
        if (!"timestamp".equals(timeColumn.cqlType())) {
            throw new IllegalArgumentException("'time' column must be of type 'timestamp'");
        }
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }

    public ColumnDefinition getColumn(String name) {
        return columnMap.get(name);
    }

    public boolean hasColumn(String name) {
        return columnMap.containsKey(name);
    }

    public List<ColumnDefinition> getPrimaryKeyColumns() {
        return columns.stream()
                .filter(ColumnDefinition::isPrimaryKey)
                .collect(Collectors.toList());
    }

    public List<ColumnDefinition> getPartitionKeyColumns() {
        return columns.stream()
                .filter(ColumnDefinition::isPartitionKey)
                .collect(Collectors.toList());
    }

    public List<ColumnDefinition> getClusteringKeyColumns() {
        return columns.stream()
                .filter(ColumnDefinition::isClusteringKey)
                .collect(Collectors.toList());
    }

    public List<ColumnDefinition> getRegularColumns() {
        return columns.stream()
                .filter(col -> !col.isPrimaryKey())
                .collect(Collectors.toList());
    }

    /**
     * Generates the CREATE TABLE CQL statement for this schema.
     */
    public String generateCreateTableStatement(String tableOptions) {
        List<ColumnDefinition> partitionKeys = getPartitionKeyColumns();
        List<ColumnDefinition> clusteringKeys = getClusteringKeyColumns();

        // Generate column definitions
        String columnDefinitions = columns.stream()
                .map(column -> "    %s %s".formatted(column.name(), column.cqlType()))
                .reduce((a, b) -> a + ",\n" + b)
                .orElse("");

        // Generate primary key definition
        String primaryKeyDef = generatePrimaryKeyDefinition(partitionKeys, clusteringKeys);

        // Generate table options
        String optionsClause = (tableOptions != null && !tableOptions.trim().isEmpty())
                ? ") WITH " + tableOptions
                : ")";

        return """
            CREATE TABLE IF NOT EXISTS %s (
            %s
                PRIMARY KEY %s
            %s
            """.formatted(tableName, columnDefinitions, primaryKeyDef, optionsClause).trim();
    }

    /**
     * Generates the primary key definition part of the CREATE TABLE statement.
     */
    private String generatePrimaryKeyDefinition(List<ColumnDefinition> partitionKeys,
                                              List<ColumnDefinition> clusteringKeys) {
        if (partitionKeys.size() == 1 && clusteringKeys.isEmpty()) {
            return "(%s)".formatted(partitionKeys.get(0).name());
        } else {
            // Composite primary key
            StringBuilder pkDef = new StringBuilder("(");

            // Add partition keys
            String partitionKeyNames = partitionKeys.stream()
                    .map(ColumnDefinition::name)
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("");
            pkDef.append(partitionKeyNames);
            pkDef.append(")");

            // Add clustering keys if present
            if (!clusteringKeys.isEmpty()) {
                String clusteringKeyNames = clusteringKeys.stream()
                        .map(ColumnDefinition::name)
                        .reduce((a, b) -> a + ", " + b)
                        .orElse("");
                pkDef.append(", ").append(clusteringKeyNames);
            }

            return pkDef.toString();
        }
    }

    public static Builder builder(String tableName) {
        return new Builder(tableName);
    }

    public static final class Builder {
        private final String tableName;
        private final List<ColumnDefinition> columns = new ArrayList<>();

        private Builder(String tableName) {
            this.tableName = tableName;
        }

        public Builder addColumn(ColumnDefinition column) {
            this.columns.add(column);
            return this;
        }

        public Builder addColumn(String name, String cqlType) {
            return addColumn(ColumnDefinition.builder(name, cqlType).build());
        }

        public Builder addPrimaryKeyColumn(String name, String cqlType) {
            return addColumn(ColumnDefinition.builder(name, cqlType).primaryKey().build());
        }

        public Builder addPartitionKeyColumn(String name, String cqlType) {
            return addColumn(ColumnDefinition.builder(name, cqlType).partitionKey().build());
        }

        public Builder addClusteringKeyColumn(String name, String cqlType) {
            return addColumn(ColumnDefinition.builder(name, cqlType).clusteringKey().build());
        }

        public CassandraSchema build() {
            return new CassandraSchema(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraSchema that = (CassandraSchema) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columns);
    }

    @Override
    public String toString() {
        return "CassandraSchema{" +
                "tableName='" + tableName + '\'' +
                ", columns=" + columns +
                '}';
    }
}
