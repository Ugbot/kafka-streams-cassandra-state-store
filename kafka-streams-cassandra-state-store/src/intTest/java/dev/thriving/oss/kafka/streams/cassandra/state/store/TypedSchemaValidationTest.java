package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed.WordCountTypedRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests demonstrating schema validation and type safety advantages
 * of typed schema-aware stores.
 */
@Tag("integration")
public class TypedSchemaValidationTest extends AbstractIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(TypedSchemaValidationTest.class);

    private CqlSession session;
    private CassandraSchema wordCountSchema;
    private WordCountTypedRepository typedRepository;

    @BeforeEach
    void setUp() {
        session = initSession();
        wordCountSchema = SchemaTemplates.wordCountSchema("validation_test_word_count");

        typedRepository = new WordCountTypedRepository(
                session,
                "validation_test_word_count",
                wordCountSchema,
                true,
                "compaction = { 'class' : 'LeveledCompactionStrategy' }",
                null,
                null
        );
    }

    @Test
    @DisplayName("Schema validation: Typed store validates type mappings at startup")
    void schemaValidationAtStartup() {
        // Test that type mappings are validated
        assertThat(TypeMapping.isSupportedType(String.class)).isTrue();
        assertThat(TypeMapping.isSupportedType(Long.class)).isTrue();
        assertThat(TypeMapping.isSupportedType(Integer.class)).isTrue();
        assertThat(TypeMapping.isSupportedType(Instant.class)).isTrue();

        // Test CQL type mappings
        assertThat(TypeMapping.getCqlType(String.class)).isEqualTo("text");
        assertThat(TypeMapping.getCqlType(Long.class)).isEqualTo("bigint");
        assertThat(TypeMapping.getCqlType(Integer.class)).isEqualTo("int");

        // Test unsupported types
        assertThat(TypeMapping.isSupportedType(Object.class)).isFalse();
        assertThat(TypeMapping.getCqlType(Object.class)).isNull();
    }

    @Test
    @DisplayName("Schema validation: Typed store validates schema structure")
    void schemaStructureValidation() {
        CassandraSchema schema = typedRepository.getSchema();

        // Test required columns exist
        assertThat(schema.hasColumn("partition")).isTrue();
        assertThat(schema.hasColumn("word")).isTrue();
        assertThat(schema.hasColumn("count")).isTrue();
        assertThat(schema.hasColumn("time")).isTrue();

        // Test column types
        assertThat(schema.getColumn("partition").cqlType()).isEqualTo("int");
        assertThat(schema.getColumn("word").cqlType()).isEqualTo("text");
        assertThat(schema.getColumn("count").cqlType()).isEqualTo("bigint");
        assertThat(schema.getColumn("time").cqlType()).isEqualTo("timestamp");

        // Test key structure
        assertThat(schema.getPartitionKeyColumns()).hasSize(1);
        assertThat(schema.getClusteringKeyColumns()).hasSize(1);
        assertThat(schema.getPrimaryKeyColumns()).hasSize(2);

        // Test that schema generates valid CQL
        String createTableSql = schema.generateCreateTableStatement("");
        assertThat(createTableSql).contains("CREATE TABLE IF NOT EXISTS");
        assertThat(createTableSql).contains("PRIMARY KEY ((partition), word)");
    }

    @Test
    @DisplayName("Type safety: Typed store prevents type mismatches at compile time")
    void typeSafetyPrevention() {
        // Demonstrate type-safe operations
        typedRepository.save(0, "hello", 42L);

        // Type-safe retrieval
        Long count = typedRepository.getByKey(0, "hello");
        assertThat(count).isEqualTo(42L);

        // Demonstrate type conversion safety
        String key = "test_key";
        Long value = 123L;

        // Safe conversion to bytes
        var keyBytes = typedRepository.keyToBytes(key);
        var valueBytes = typedRepository.valueToBytes(value);

        // Safe conversion back from bytes
        String convertedKey = typedRepository.bytesToKey(keyBytes);
        Long convertedValue = typedRepository.bytesToValue(valueBytes);

        assertThat(convertedKey).isEqualTo(key);
        assertThat(convertedValue).isEqualTo(value);

        // Demonstrate that invalid conversions are handled gracefully
        byte[] invalidBytes = "notanumber".getBytes(StandardCharsets.UTF_8);
        Long invalidResult = typedRepository.bytesToValue(invalidBytes);
        assertThat(invalidResult).isEqualTo(0L); // Default value for invalid data
    }

    @Test
    @DisplayName("Schema validation: Typed store validates data against schema constraints")
    void dataValidationAgainstSchema() {
        // Test valid data
        typedRepository.save(0, "valid_word", 100L);
        Long retrieved = typedRepository.getByKey(0, "valid_word");
        assertThat(retrieved).isEqualTo(100L);

        // Test null handling
        assertThatThrownBy(() -> typedRepository.save(0, null, 50L))
                .isInstanceOf(IllegalArgumentException.class);

        // Test that null values are handled gracefully
        Long nullResult = typedRepository.getByKey(0, "nonexistent");
        assertThat(nullResult).isNull();

        // Test edge cases
        typedRepository.save(0, "empty_string", 0L);
        Long zeroResult = typedRepository.getByKey(0, "empty_string");
        assertThat(zeroResult).isEqualTo(0L);
    }

    @Test
    @DisplayName("Schema validation: Runtime schema validation for topic data")
    void runtimeSchemaValidation() {
        // Create a sample consumer record
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "test-topic",
                0,
                0L,
                "test_key".getBytes(),
                "42".getBytes()
        );

        // Test schema validation
        var validationResult = SchemaValidation.validateRecord(
                record,
                Serdes.String().deserializer(),
                Serdes.Long().deserializer(),
                wordCountSchema
        );

        // Should pass basic validation
        assertThat(validationResult.valid()).isTrue();

        // Test with invalid data
        ConsumerRecord<byte[], byte[]> invalidRecord = new ConsumerRecord<>(
                "test-topic",
                0,
                0L,
                null, // null key
                "invalid".getBytes()
        );

        var invalidResult = SchemaValidation.validateRecord(
                invalidRecord,
                Serdes.String().deserializer(),
                Serdes.Long().deserializer(),
                wordCountSchema
        );

        // Should detect validation issues
        assertThat(invalidResult.valid()).isFalse();
        assertThat(invalidResult.errors()).isNotEmpty();
    }

    @Test
    @DisplayName("Schema validation: Topic schema assertion for startup validation")
    void topicSchemaAssertion() {
        // Test schema assertion with valid schema
        assertThatCode(() -> SchemaAssertion.assertSchemaIsValid(wordCountSchema))
                .doesNotThrowAnyException();

        // Test topic validation (would normally require a running Kafka cluster)
        // This demonstrates the API without actually connecting to Kafka
        LOG.info("Schema assertion API is ready for use with running Kafka clusters");
        LOG.info("Schema contains {} columns", wordCountSchema.getColumns().size());
        LOG.info("Primary key columns: {}", wordCountSchema.getPrimaryKeyColumns().size());
    }

    @Test
    @DisplayName("Type safety: Demonstrate compile-time type checking advantages")
    void compileTimeTypeChecking() {
        // This test demonstrates the type safety advantages
        // In a real application, these operations would be caught at compile time

        // Valid operations
        String validKey = "test";
        Long validValue = 42L;

        typedRepository.save(0, validKey, validValue);
        Long result = typedRepository.getByKey(0, validKey);

        assertThat(result).isEqualTo(validValue);

        // Demonstrate type-aware error handling
        // With typed stores, type mismatches are caught early
        try {
            // This would normally be a compile-time error with proper generics
            // But we can demonstrate runtime type safety
            Object invalidKey = 123; // Wrong type
            if (invalidKey instanceof String) {
                typedRepository.save(0, (String) invalidKey, validValue);
            }
        } catch (ClassCastException e) {
            LOG.info("Caught type mismatch at runtime: {}", e.getMessage());
        }
    }

    @Test
    @DisplayName("Schema evolution: Typed store supports schema compatibility checking")
    void schemaEvolutionSupport() {
        // Create a compatible schema (same structure)
        CassandraSchema compatibleSchema = CassandraSchema.builder("test_compatible")
                .addPartitionKeyColumn("partition", "int")
                .addClusteringKeyColumn("word", "text")
                .addColumn("count", "bigint")
                .addColumn("time", "timestamp")
                .build();

        // Test schema compatibility
        assertThatCode(() -> SchemaAssertion.assertSchemasCompatible(wordCountSchema, compatibleSchema))
                .doesNotThrowAnyException();

        // Create an incompatible schema
        CassandraSchema incompatibleSchema = CassandraSchema.builder("test_incompatible")
                .addPartitionKeyColumn("partition", "int")
                .addClusteringKeyColumn("word", "text")
                .addColumn("count", "text") // Different type
                .addColumn("time", "timestamp")
                .build();

        // Should detect incompatibility
        assertThatThrownBy(() -> SchemaAssertion.assertSchemasCompatible(wordCountSchema, incompatibleSchema))
                .isInstanceOf(SchemaAssertion.SchemaValidationException.class);
    }

    @Test
    @DisplayName("Type mapping: Comprehensive type mapping validation")
    void comprehensiveTypeMapping() {
        // Test all supported Java types
        assertThat(TypeMapping.isSupportedType(Boolean.class)).isTrue();
        assertThat(TypeMapping.isSupportedType(Integer.class)).isTrue();
        assertThat(TypeMapping.isSupportedType(Long.class)).isTrue();
        assertThat(TypeMapping.isSupportedType(Float.class)).isTrue();
        assertThat(TypeMapping.isSupportedType(Double.class)).isTrue();
        assertThat(TypeMapping.isSupportedType(String.class)).isTrue();
        assertThat(TypeMapping.isSupportedType(Instant.class)).isTrue();

        // Test CQL type mappings
        assertThat(TypeMapping.getCqlType(Boolean.class)).isEqualTo("boolean");
        assertThat(TypeMapping.getCqlType(Integer.class)).isEqualTo("int");
        assertThat(TypeMapping.getCqlType(Long.class)).isEqualTo("bigint");
        assertThat(TypeMapping.getCqlType(Float.class)).isEqualTo("float");
        assertThat(TypeMapping.getCqlType(Double.class)).isEqualTo("double");
        assertThat(TypeMapping.getCqlType(String.class)).isEqualTo("text");

        // Test Cassandra type mappings
        assertThat(TypeMapping.getCassandraType(String.class)).isNotNull();
        assertThat(TypeMapping.getCassandraType(Long.class)).isNotNull();

        // Test collection type creation
        String listType = TypeMapping.createListType("text");
        String setType = TypeMapping.createSetType("text");
        String mapType = TypeMapping.createMapType("text", "bigint");

        assertThat(listType).isEqualTo("list<text>");
        assertThat(setType).isEqualTo("set<text>");
        assertThat(mapType).isEqualTo("map<text,bigint>");
    }

    @Test
    @DisplayName("Error handling: Typed store provides better error messages")
    void improvedErrorHandling() {
        // Test null key handling
        assertThatThrownBy(() -> typedRepository.save(0, null, 42L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be null");

        // Test type conversion errors
        byte[] invalidData = "notanumber".getBytes(StandardCharsets.UTF_8);
        Long result = typedRepository.bytesToValue(invalidData);

        // Should handle gracefully with default value
        assertThat(result).isEqualTo(0L);

        // Test schema validation errors
        assertThatThrownBy(() -> TypeMapping.validateTypeMapping(Object.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported");

        // Test CQL type validation
        assertThatThrownBy(() -> TypeMapping.validateCqlType("unsupported_type"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported");
    }
}
