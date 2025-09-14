package dev.thriving.oss.kafka.streams.cassandra.state.store.schema;

/**
 * Provides predefined schema templates for common Kafka Streams use cases.
 * These templates can be used as starting points and customized as needed.
 */
public final class SchemaTemplates {

    private SchemaTemplates() {
        // Utility class
    }

    /**
     * Creates a schema for a word count state store with typed columns.
     * Uses TEXT for words and BIGINT for counts instead of generic BLOBs.
     */
    public static CassandraSchema wordCountSchema(String tableName) {
        return CassandraSchema.builder(tableName)
                .addPartitionKeyColumn("partition", "int")
                .addClusteringKeyColumn("word", "text")
                .addColumn("count", "bigint")
                .addColumn("time", "timestamp")
                .build();
    }

    /**
     * Creates a schema for a user profile state store.
     * Demonstrates structured data with multiple typed fields.
     */
    public static CassandraSchema userProfileSchema(String tableName) {
        return CassandraSchema.builder(tableName)
                .addPartitionKeyColumn("partition", "int")
                .addClusteringKeyColumn("user_id", "uuid")
                .addColumn("username", "text")
                .addColumn("email", "text")
                .addColumn("first_name", "text")
                .addColumn("last_name", "text")
                .addColumn("age", "int")
                .addColumn("registration_date", "timestamp")
                .addColumn("last_login", "timestamp")
                .addColumn("is_active", "boolean")
                .addColumn("time", "timestamp")
                .build();
    }

    /**
     * Creates a schema for an e-commerce order state store.
     * Shows how to handle complex business data types.
     */
    public static CassandraSchema orderSchema(String tableName) {
        return CassandraSchema.builder(tableName)
                .addPartitionKeyColumn("partition", "int")
                .addClusteringKeyColumn("order_id", "uuid")
                .addColumn("customer_id", "uuid")
                .addColumn("order_date", "timestamp")
                .addColumn("total_amount", "decimal")
                .addColumn("currency", "text")
                .addColumn("status", "text") // e.g., 'pending', 'confirmed', 'shipped', 'delivered'
                .addColumn("shipping_address", "text")
                .addColumn("billing_address", "text")
                .addColumn("items", "frozen<list<frozen<tuple<text, int, decimal>>>>") // product_name, quantity, price
                .addColumn("time", "timestamp")
                .build();
    }

    /**
     * Creates a schema for a global key-value store with typed data.
     * Uses the key as the primary key (no partition column needed).
     */
    public static CassandraSchema globalTypedKeyValueSchema(String tableName) {
        return CassandraSchema.builder(tableName)
                .addPartitionKeyColumn("key", "text")
                .addColumn("value", "text")
                .addColumn("time", "timestamp")
                .build();
    }

    /**
     * Creates a schema for a time-series metrics store.
     * Optimized for time-based queries and aggregations.
     */
    public static CassandraSchema timeSeriesMetricsSchema(String tableName) {
        return CassandraSchema.builder(tableName)
                .addPartitionKeyColumn("partition", "int")
                .addPartitionKeyColumn("metric_name", "text")
                .addClusteringKeyColumn("timestamp", "timestamp")
                .addColumn("value", "double")
                .addColumn("tags", "map<text, text>")
                .addColumn("time", "timestamp")
                .build();
    }

    /**
     * Creates a schema for a session store with TTL support.
     * Suitable for temporary session data that should expire.
     */
    public static CassandraSchema sessionStoreSchema(String tableName) {
        return CassandraSchema.builder(tableName)
                .addPartitionKeyColumn("partition", "int")
                .addClusteringKeyColumn("session_id", "uuid")
                .addColumn("user_id", "uuid")
                .addColumn("session_data", "text")
                .addColumn("created_at", "timestamp")
                .addColumn("expires_at", "timestamp")
                .addColumn("time", "timestamp")
                .build();
    }

    /**
     * Creates a schema for a product catalog with rich metadata.
     */
    public static CassandraSchema productCatalogSchema(String tableName) {
        return CassandraSchema.builder(tableName)
                .addPartitionKeyColumn("partition", "int")
                .addClusteringKeyColumn("product_id", "uuid")
                .addColumn("name", "text")
                .addColumn("description", "text")
                .addColumn("category", "text")
                .addColumn("price", "decimal")
                .addColumn("currency", "text")
                .addColumn("inventory_count", "int")
                .addColumn("tags", "set<text>")
                .addColumn("attributes", "map<text, text>")
                .addColumn("images", "list<text>") // URLs to product images
                .addColumn("created_at", "timestamp")
                .addColumn("updated_at", "timestamp")
                .addColumn("time", "timestamp")
                .build();
    }

    /**
     * Creates a schema for storing JSON documents with typed metadata.
     * Useful when you want to store flexible JSON data with some typed fields.
     */
    public static CassandraSchema jsonDocumentSchema(String tableName) {
        return CassandraSchema.builder(tableName)
                .addPartitionKeyColumn("partition", "int")
                .addClusteringKeyColumn("document_id", "uuid")
                .addColumn("document_type", "text")
                .addColumn("version", "int")
                .addColumn("content", "text") // JSON content
                .addColumn("size_bytes", "bigint")
                .addColumn("checksum", "text")
                .addColumn("created_by", "text")
                .addColumn("created_at", "timestamp")
                .addColumn("time", "timestamp")
                .build();
    }
}
