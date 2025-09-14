package dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.CassandraSchema;

/**
 * Factory for creating typed Cassandra repositories based on schema types.
 * Provides a way to automatically create the appropriate typed repository
 * implementation for different schema patterns.
 */
public class TypedRepositoryFactory {

    /**
     * Creates a typed repository based on the schema structure.
     * Automatically detects the schema type and creates the appropriate repository.
     *
     * @param session Cassandra session
     * @param tableName table name
     * @param schema schema definition
     * @param createTable whether to create the table
     * @param tableOptions table options
     * @param ddlExecutionProfile DDL execution profile
     * @param dmlExecutionProfile DML execution profile
     * @return appropriate typed repository implementation
     */
    public static TypedCassandraKeyValueStoreRepository<?, ?> createRepository(
            CqlSession session,
            String tableName,
            CassandraSchema schema,
            boolean createTable,
            String tableOptions,
            String ddlExecutionProfile,
            String dmlExecutionProfile) {

        SchemaType schemaType = determineSchemaType(schema);

        return switch (schemaType) {
            case WORD_COUNT -> new WordCountTypedRepository(
                    session, tableName, schema, createTable,
                    tableOptions, ddlExecutionProfile, dmlExecutionProfile);

            case USER_PROFILE -> {
                // TODO: Implement UserProfileTypedRepository
                throw new UnsupportedOperationException(
                    "UserProfileTypedRepository not yet implemented for schema: " + schema.getTableName());
            }

            case ORDER -> {
                // TODO: Implement OrderTypedRepository
                throw new UnsupportedOperationException(
                    "OrderTypedRepository not yet implemented for schema: " + schema.getTableName());
            }

            case METRICS -> {
                // TODO: Implement MetricsTypedRepository
                throw new UnsupportedOperationException(
                    "MetricsTypedRepository not yet implemented for schema: " + schema.getTableName());
            }

            case UNKNOWN -> throw new UnsupportedOperationException(
                "No typed repository implementation available for unknown schema type: " + schema.getTableName());
        };
    }

    /**
     * Determines the schema type using pattern matching.
     */
    private static SchemaType determineSchemaType(CassandraSchema schema) {
        // Word count schema pattern
        if (schema.hasColumn("word") &&
            schema.hasColumn("count") &&
            "text".equals(schema.getColumn("word").cqlType()) &&
            "bigint".equals(schema.getColumn("count").cqlType())) {
            return SchemaType.WORD_COUNT;
        }

        // User profile schema pattern
        if (schema.hasColumn("user_id") &&
            schema.hasColumn("username") &&
            schema.hasColumn("email")) {
            return SchemaType.USER_PROFILE;
        }

        // Order schema pattern
        if (schema.hasColumn("order_id") &&
            schema.hasColumn("customer_id") &&
            schema.hasColumn("total_amount")) {
            return SchemaType.ORDER;
        }

        // Metrics schema pattern
        if (schema.hasColumn("metric_name") &&
            schema.hasColumn("timestamp") &&
            schema.hasColumn("value")) {
            return SchemaType.METRICS;
        }

        return SchemaType.UNKNOWN;
    }

    /**
     * Schema type enumeration for pattern matching.
     */
    private enum SchemaType {
        WORD_COUNT,
        USER_PROFILE,
        ORDER,
        METRICS,
        UNKNOWN
    }
}
