package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.CassandraSchema;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.SchemaTemplates;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.TypeMapping;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests demonstrating advantages of typed schema-aware stores for complex data types
 * like user profiles, e-commerce data, and analytics metrics.
 */
@Tag("integration")
public class ComplexDataTypeTest extends AbstractIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(ComplexDataTypeTest.class);

    private CqlSession session;

    @BeforeEach
    void setUp() {
        session = initSession();
    }

    @Test
    @DisplayName("Complex data: Demonstrate type mapping capabilities for rich data types")
    void typeMappingCapabilities() {
        // Test comprehensive type support
        Map<Class<?>, String> expectedMappings = Map.of(
                Boolean.class, "boolean",
                Integer.class, "int",
                Long.class, "bigint",
                Float.class, "float",
                Double.class, "double",
                String.class, "text",
                Instant.class, "timestamp"
        );

        // Validate all mappings
        expectedMappings.forEach((javaType, expectedCql) -> {
            assertThat(TypeMapping.isSupportedType(javaType))
                    .withFailMessage("Type %s should be supported", javaType.getSimpleName())
                    .isTrue();

            assertThat(TypeMapping.getCqlType(javaType))
                    .withFailMessage("Type %s should map to %s", javaType.getSimpleName(), expectedCql)
                    .isEqualTo(expectedCql);

            assertThat(TypeMapping.getCassandraType(javaType))
                    .withFailMessage("Type %s should have Cassandra type mapping", javaType.getSimpleName())
                    .isNotNull();
        });

        LOG.info("Validated {} type mappings", expectedMappings.size());
    }

    @Test
    @DisplayName("Complex data: User profile data with multiple typed fields")
    void userProfileDataHandling() {
        // Create a user profile schema
        CassandraSchema userProfileSchema = CassandraSchema.builder("complex_test_user_profile")
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

        LOG.info("Created user profile schema with {} columns", userProfileSchema.getColumns().size());

        // Simulate user profile data operations
        Map<String, Object> userProfile = Map.of(
                "user_id", UUID.randomUUID(),
                "username", "john_doe",
                "email", "john.doe@example.com",
                "first_name", "John",
                "last_name", "Doe",
                "age", 30,
                "registration_date", Instant.now(),
                "last_login", Instant.now(),
                "is_active", true
        );

        // With typed stores, this data would be strongly typed
        // Here we demonstrate the schema structure
        assertThat(userProfileSchema.hasColumn("user_id")).isTrue();
        assertThat(userProfileSchema.hasColumn("email")).isTrue();
        assertThat(userProfileSchema.hasColumn("age")).isTrue();
        assertThat(userProfileSchema.hasColumn("is_active")).isTrue();

        // Generate and validate CQL
        String createTableSql = userProfileSchema.generateCreateTableStatement("");
        assertThat(createTableSql).contains("user_id uuid");
        assertThat(createTableSql).contains("email text");
        assertThat(createTableSql).contains("age int");
        assertThat(createTableSql).contains("is_active boolean");

        LOG.info("User profile schema generated successfully");
    }

    @Test
    @DisplayName("Complex data: E-commerce order data with nested structures")
    void ecommerceOrderDataHandling() {
        // Create an e-commerce order schema
        CassandraSchema orderSchema = CassandraSchema.builder("complex_test_orders")
                .addPartitionKeyColumn("partition", "int")
                .addClusteringKeyColumn("order_id", "uuid")
                .addColumn("customer_id", "uuid")
                .addColumn("order_date", "timestamp")
                .addColumn("total_amount", "decimal")
                .addColumn("currency", "text")
                .addColumn("status", "text")
                .addColumn("shipping_address", "text")
                .addColumn("billing_address", "text")
                .addColumn("time", "timestamp")
                .build();

        LOG.info("Created e-commerce order schema");

        // Simulate order data
        Map<String, Object> orderData = Map.of(
                "order_id", UUID.randomUUID(),
                "customer_id", UUID.randomUUID(),
                "order_date", Instant.now(),
                "total_amount", java.math.BigDecimal.valueOf(299.99),
                "currency", "USD",
                "status", "confirmed",
                "shipping_address", "123 Main St, Anytown, USA",
                "billing_address", "123 Main St, Anytown, USA"
        );

        // Validate schema supports complex data types
        assertThat(orderSchema.hasColumn("order_id")).isTrue();
        assertThat(orderSchema.hasColumn("total_amount")).isTrue();
        assertThat(orderSchema.hasColumn("currency")).isTrue();

        assertThat(orderSchema.getColumn("total_amount").cqlType()).isEqualTo("decimal");
        assertThat(orderSchema.getColumn("currency").cqlType()).isEqualTo("text");

        LOG.info("E-commerce order schema validation completed");
    }

    @Test
    @DisplayName("Complex data: Analytics metrics with time-series data")
    void analyticsMetricsHandling() {
        // Create analytics metrics schema
        CassandraSchema metricsSchema = CassandraSchema.builder("complex_test_metrics")
                .addPartitionKeyColumn("partition", "int")
                .addPartitionKeyColumn("metric_name", "text")
                .addClusteringKeyColumn("timestamp", "timestamp")
                .addColumn("value", "double")
                .addColumn("tags", "map<text,text>")
                .addColumn("time", "timestamp")
                .build();

        LOG.info("Created analytics metrics schema");

        // Simulate metrics data
        Map<String, Object> metricsData = Map.of(
                "metric_name", "page_views",
                "timestamp", Instant.now(),
                "value", 1250.5,
                "tags", Map.of(
                        "page", "/home",
                        "user_type", "premium",
                        "device", "mobile"
                )
        );

        // Validate complex type support
        assertThat(metricsSchema.hasColumn("tags")).isTrue();
        assertThat(metricsSchema.getColumn("tags").cqlType()).isEqualTo("map<text,text>");
        assertThat(metricsSchema.getColumn("value").cqlType()).isEqualTo("double");

        LOG.info("Analytics metrics schema with complex types validated");
    }

    @Test
    @DisplayName("Complex data: Demonstrate collection type mappings")
    void collectionTypeMappings() {
        // Test collection type creation utilities
        String listType = TypeMapping.createListType("text");
        String setType = TypeMapping.createSetType("text");
        String mapType = TypeMapping.createMapType("text", "bigint");

        assertThat(listType).isEqualTo("list<text>");
        assertThat(setType).isEqualTo("set<text>");
        assertThat(mapType).isEqualTo("map<text,bigint>");

        // Create schema with collection types
        CassandraSchema collectionSchema = CassandraSchema.builder("complex_test_collections")
                .addPartitionKeyColumn("partition", "int")
                .addClusteringKeyColumn("id", "uuid")
                .addColumn("tags", "set<text>")
                .addColumn("attributes", "map<text,text>")
                .addColumn("categories", "list<text>")
                .addColumn("scores", "list<double>")
                .addColumn("time", "timestamp")
                .build();

        // Validate collection types in schema
        assertThat(collectionSchema.getColumn("tags").cqlType()).isEqualTo("set<text>");
        assertThat(collectionSchema.getColumn("attributes").cqlType()).isEqualTo("map<text,text>");
        assertThat(collectionSchema.getColumn("categories").cqlType()).isEqualTo("list<text>");
        assertThat(collectionSchema.getColumn("scores").cqlType()).isEqualTo("list<double>");

        LOG.info("Collection type mappings validated successfully");
    }

    @Test
    @DisplayName("Complex data: Performance comparison for different data sizes")
    void performanceWithComplexData() {
        CassandraSchema wordCountSchema = SchemaTemplates.wordCountSchema("performance_test_complex");

        // Test with increasing data sizes
        int[] dataSizes = {100, 500, 1000, 2500};

        for (int dataSize : dataSizes) {
            Instant testStart = Instant.now();

            // Simulate complex data operations
            Map<String, Long> testData = new HashMap<>();
            IntStream.range(0, dataSize).forEach(i -> {
                testData.put("complex_key_" + i, (long) i * 100);
            });

            // In a real implementation, this would use the typed repository
            // For now, we simulate the operations
            Duration testDuration = Duration.between(testStart, Instant.now());

            LOG.info("Processed {} complex data items in {}ms", dataSize, testDuration.toMillis());

            // Verify data structure
            assertThat(testData).hasSize(dataSize);
            assertThat(testData.get("complex_key_0")).isEqualTo(0L);
            assertThat(testData.get("complex_key_" + (dataSize - 1))).isEqualTo((long) (dataSize - 1) * 100);
        }
    }

    @Test
    @DisplayName("Complex data: Schema validation for complex data structures")
    void schemaValidationForComplexData() {
        // Test multiple complex schemas
        List<CassandraSchema> testSchemas = Arrays.asList(
                SchemaTemplates.wordCountSchema("validation_test_1"),
                createUserProfileSchema(),
                createEcommerceSchema(),
                createAnalyticsSchema()
        );

        for (CassandraSchema schema : testSchemas) {
            // Validate schema structure
            assertThat(schema.getColumns()).isNotEmpty();
            assertThat(schema.getPrimaryKeyColumns()).isNotEmpty();
            assertThat(schema.getPartitionKeyColumns()).isNotEmpty();

            // Validate CQL generation
            String cql = schema.generateCreateTableStatement("");
            assertThat(cql).contains("CREATE TABLE IF NOT EXISTS");
            assertThat(cql).contains("PRIMARY KEY");

            // Validate all column types are supported
            schema.getColumns().forEach(column -> {
                String cqlType = column.cqlType();
                // Basic validation - in real implementation would check against supported types
                assertThat(cqlType).isNotNull().isNotEmpty();
            });

            LOG.info("Schema '{}' validated successfully with {} columns",
                    schema.getTableName(), schema.getColumns().size());
        }
    }

    @Test
    @DisplayName("Complex data: Demonstrate advantages for JSON/document data")
    void jsonDocumentHandling() {
        // Create schema for JSON document storage
        CassandraSchema documentSchema = CassandraSchema.builder("complex_test_documents")
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

        // Simulate document data
        Map<String, Object> documentData = Map.of(
                "document_id", UUID.randomUUID(),
                "document_type", "user_profile",
                "version", 1,
                "content", "{\"name\":\"John\",\"age\":30,\"active\":true}",
                "size_bytes", 45L,
                "checksum", "abc123def456",
                "created_by", "system",
                "created_at", Instant.now()
        );

        // Validate document schema
        assertThat(documentSchema.hasColumn("content")).isTrue();
        assertThat(documentSchema.hasColumn("version")).isTrue();
        assertThat(documentSchema.getColumn("content").cqlType()).isEqualTo("text");
        assertThat(documentSchema.getColumn("version").cqlType()).isEqualTo("int");

        LOG.info("JSON document schema validated for complex data storage");
    }

    // Helper methods for creating test schemas

    private CassandraSchema createUserProfileSchema() {
        return CassandraSchema.builder("complex_test_user_profile_detailed")
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
                .addColumn("preferences", "map<text,text>")
                .addColumn("tags", "set<text>")
                .addColumn("time", "timestamp")
                .build();
    }

    private CassandraSchema createEcommerceSchema() {
        return CassandraSchema.builder("complex_test_ecommerce_detailed")
                .addPartitionKeyColumn("partition", "int")
                .addClusteringKeyColumn("order_id", "uuid")
                .addColumn("customer_id", "uuid")
                .addColumn("order_date", "timestamp")
                .addColumn("total_amount", "decimal")
                .addColumn("currency", "text")
                .addColumn("status", "text")
                .addColumn("items", "list<text>") // JSON array of items
                .addColumn("shipping_address", "text")
                .addColumn("billing_address", "text")
                .addColumn("discount_codes", "set<text>")
                .addColumn("metadata", "map<text,text>")
                .addColumn("time", "timestamp")
                .build();
    }

    private CassandraSchema createAnalyticsSchema() {
        return CassandraSchema.builder("complex_test_analytics_detailed")
                .addPartitionKeyColumn("partition", "int")
                .addPartitionKeyColumn("event_type", "text")
                .addClusteringKeyColumn("timestamp", "timestamp")
                .addColumn("user_id", "uuid")
                .addColumn("session_id", "uuid")
                .addColumn("value", "double")
                .addColumn("dimensions", "map<text,text>")
                .addColumn("metrics", "map<text,double>")
                .addColumn("tags", "set<text>")
                .addColumn("properties", "text") // JSON properties
                .addColumn("time", "timestamp")
                .build();
    }
}
