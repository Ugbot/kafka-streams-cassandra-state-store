package dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.AbstractCassandraStateStoreRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.CassandraSchema;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for typed Cassandra repositories.
 * Provides common functionality for schema-aware data operations.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public abstract class AbstractTypedCassandraRepository<K, V>
        extends AbstractCassandraStateStoreRepository
        implements TypedCassandraKeyValueStoreRepository<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTypedCassandraRepository.class);

    protected final CassandraSchema schema;
    protected final String keyspaceName;

    // Prepared statements will be initialized by subclasses
    protected PreparedStatement insertStatement;
    protected PreparedStatement selectByKeyStatement;
    protected PreparedStatement deleteByKeyStatement;
    protected PreparedStatement selectAllStatement;
    protected PreparedStatement selectAllReversedStatement;
    protected PreparedStatement selectCountStatement;
    protected PreparedStatement selectCountByPartitionStatement;

    public AbstractTypedCassandraRepository(CqlSession session,
                                          String tableName,
                                          CassandraSchema schema,
                                          boolean createTable,
                                          String tableOptions,
                                          String ddlExecutionProfile,
                                          String dmlExecutionProfile) {
        super(session, tableName, createTable, tableOptions, ddlExecutionProfile, dmlExecutionProfile);
        this.schema = schema;
        this.keyspaceName = extractKeyspaceName(tableName);
        initPreparedStatements(tableName);
    }

    @Override
    public CassandraSchema getSchema() {
        return schema;
    }

    @Override
    protected String buildCreateTableQuery(String tableName, String tableOptions) {
        return schema.generateCreateTableStatement(tableOptions);
    }

    @Override
    protected abstract void initPreparedStatements(String tableName);

    /**
     * Extracts the keyspace name from a fully qualified table name (keyspace.table).
     */
    private String extractKeyspaceName(String tableName) {
        if (tableName.contains(".")) {
            return tableName.substring(0, tableName.indexOf("."));
        }
        return null; // Use session default keyspace
    }

    /**
     * Creates a parameterized INSERT statement for the schema.
     */
    protected String buildInsertStatement(String tableName) {
        String columns = schema.getColumns().stream()
                .map(column -> column.name())
                .reduce((a, b) -> a + ", " + b)
                .orElse("");

        String placeholders = "?".repeat(schema.getColumns().size())
                .chars()
                .mapToObj(c -> String.valueOf((char) c))
                .reduce((a, b) -> a + ", " + b)
                .orElse("");

        return "INSERT INTO %s (%s) VALUES (%s)".formatted(tableName, columns, placeholders);
    }

    /**
     * Creates a parameterized SELECT statement for retrieving by key.
     */
    protected String buildSelectByKeyStatement(String tableName) {
        String whereClause = schema.getPrimaryKeyColumns().stream()
                .map(column -> column.name() + " = ?")
                .reduce((a, b) -> a + " AND " + b)
                .orElse("");

        return "SELECT * FROM %s WHERE %s".formatted(tableName, whereClause);
    }

    /**
     * Creates a parameterized DELETE statement.
     */
    protected String buildDeleteByKeyStatement(String tableName) {
        String whereClause = schema.getPrimaryKeyColumns().stream()
                .map(column -> column.name() + " = ?")
                .reduce((a, b) -> a + " AND " + b)
                .orElse("");

        return "DELETE FROM %s WHERE %s".formatted(tableName, whereClause);
    }

    /**
     * Creates a SELECT statement for all records in a partition.
     */
    protected String buildSelectAllStatement(String tableName, boolean reversed) {
        String orderBy = schema.getClusteringKeyColumns().stream()
                .findFirst()
                .map(column -> " ORDER BY " + column.name() + (reversed ? " DESC" : ""))
                .orElse("");

        return "SELECT * FROM %s WHERE partition=?%s".formatted(tableName, orderBy);
    }

    /**
     * Creates count statements.
     */
    protected String buildSelectCountStatement(String tableName, boolean byPartition) {
        String whereClause = byPartition ? " WHERE partition=?" : "";
        return "SELECT COUNT(*) FROM %s%s".formatted(tableName, whereClause);
    }

    /**
     * Binds key values to a prepared statement.
     */
    protected BoundStatement bindKeyToStatement(PreparedStatement stmt, K key, int partition) {
        Map<String, Object> keyMap = keyToColumnMap(key);
        keyMap.put("partition", partition); // Ensure partition is included

        List<Object> bindValues = schema.getPrimaryKeyColumns().stream()
                .map(column -> {
                    Object value = keyMap.get(column.name());
                    if (value == null) {
                        throw new IllegalArgumentException(
                            "Primary key column '%s' cannot be null".formatted(column.name()));
                    }
                    return value;
                })
                .toList();

        return stmt.bind(bindValues.toArray());
    }

    /**
     * Binds key and value to an INSERT statement.
     */
    protected BoundStatement bindInsertStatement(K key, V value, int partition) {
        Map<String, Object> keyMap = keyToColumnMap(key);
        Map<String, Object> valueMap = valueToColumnMap(value);

        // Combine key and value maps
        Map<String, Object> allValues = new HashMap<>();
        allValues.putAll(keyMap);
        allValues.putAll(valueMap);
        allValues.put("partition", partition);
        allValues.put("time", java.time.Instant.now()); // Always include timestamp

        List<Object> bindValues = schema.getColumns().stream()
                .map(column -> {
                    Object columnValue = allValues.get(column.name());
                    if (columnValue == null && column.isPrimaryKey() && !"partition".equals(column.name())) {
                        throw new IllegalArgumentException(
                            "Primary key column '%s' cannot be null".formatted(column.name()));
                    }
                    return columnValue;
                })
                .toList();

        return insertStatement.bind(bindValues.toArray());
    }

    /**
     * Converts a CQL Row to a Map for easier processing.
     */
    protected Map<String, Object> rowToMap(Row row) {
        Map<String, Object> result = new HashMap<>();
        for (var column : schema.getColumns()) {
            String columnName = column.name();
            try {
                // Get value based on CQL type
                Object value = getValueFromRow(row, columnName, column.cqlType());
                result.put(columnName, value);
            } catch (Exception e) {
                LOG.warn("Failed to get value for column '{}': {}", columnName, e.getMessage());
                result.put(columnName, null);
            }
        }
        return result;
    }

    /**
     * Gets a value from a CQL Row based on the column type.
     */
    private Object getValueFromRow(Row row, String columnName, String cqlType) {
        return switch (cqlType.toLowerCase()) {
            case "text", "varchar", "ascii" -> row.getString(columnName);
            case "int" -> row.getInt(columnName);
            case "bigint" -> row.getLong(columnName);
            case "float" -> row.getFloat(columnName);
            case "double" -> row.getDouble(columnName);
            case "boolean" -> row.getBoolean(columnName);
            case "timestamp" -> row.getInstant(columnName);
            case "uuid" -> row.getUuid(columnName);
            case "timeuuid" -> row.getUuid(columnName); // timeuuid is also UUID type
            case "decimal" -> row.getBigDecimal(columnName);
            case "blob" -> row.getByteBuffer(columnName);
            default -> {
                // For complex types like collections, return as-is
                if (cqlType.startsWith("list<") || cqlType.startsWith("set<") || cqlType.startsWith("map<")) {
                    yield row.getObject(columnName);
                } else if (cqlType.startsWith("frozen<")) {
                    yield row.getObject(columnName);
                } else if (cqlType.startsWith("tuple<")) {
                    yield row.getObject(columnName);
                } else {
                    // Unknown type, try to get as object
                    yield row.getObject(columnName);
                }
            }
        };
    }

    /**
     * Executes a statement with the appropriate profile.
     */
    protected ResultSet executeStatement(BoundStatement stmt) {
        if (dmlExecutionProfile != null) {
            stmt = stmt.setExecutionProfileName(dmlExecutionProfile);
        }
        return session.execute(stmt);
    }
}
