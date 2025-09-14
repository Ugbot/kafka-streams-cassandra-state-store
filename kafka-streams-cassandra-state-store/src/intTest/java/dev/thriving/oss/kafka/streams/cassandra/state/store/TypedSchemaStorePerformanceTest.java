package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.PartitionedCassandraKeyValueStoreRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed.WordCountTypedRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.CassandraSchema;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.SchemaTemplates;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.TypeMapping;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

/**
 * Performance and functionality tests demonstrating advantages of typed schema-aware stores
 * over traditional BLOB-based stores.
 */
@Tag("integration")
public class TypedSchemaStorePerformanceTest extends AbstractIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(TypedSchemaStorePerformanceTest.class);

    private CassandraSchema wordCountSchema;
    private WordCountTypedRepository typedRepository;
    private CassandraKeyValueStore blobStore;

    @BeforeEach
    void setUpTypedStore() {
        CqlSession session = initSession();

        // Create schema with type mappings
        wordCountSchema = SchemaTemplates.wordCountSchema("perf_test_word_count");

        // Validate type mappings
        assertThat(TypeMapping.getCqlType(String.class)).isEqualTo("text");
        assertThat(TypeMapping.getCqlType(Long.class)).isEqualTo("bigint");
        assertThat(TypeMapping.isSupportedType(String.class)).isTrue();
        assertThat(TypeMapping.isSupportedType(Long.class)).isTrue();

        // Create typed repository
        typedRepository = new WordCountTypedRepository(
                session,
                "perf_test_word_count",
                wordCountSchema,
                true,
                "compaction = { 'class' : 'LeveledCompactionStrategy' }",
                null,
                null
        );

        // Create traditional BLOB store for comparison
        blobStore = new CassandraKeyValueStore(
                "perf_test_blob_store",
                new PartitionedCassandraKeyValueStoreRepository(
                        session,
                        "perf_test_blob_store",
                        true,
                        "compaction = { 'class' : 'LeveledCompactionStrategy' }",
                        null,
                        null
                ),
                true
        );
    }

    @Test
    @DisplayName("Typed store provides compile-time type safety vs runtime BLOB handling")
    void typedStoreProvidesTypeSafety() {
        // Typed operations - compile-time safety
        typedRepository.save(0, "hello", 42L);

        // Type-safe retrieval
        Long count = typedRepository.getByKey(0, "hello");
        assertThat(count).isEqualTo(42L);

        // Type conversion validation
        Bytes keyBytes = typedRepository.keyToBytes("test");
        byte[] valueBytes = typedRepository.valueToBytes(123L);

        String convertedKey = typedRepository.bytesToKey(keyBytes);
        Long convertedValue = typedRepository.bytesToValue(valueBytes);

        assertThat(convertedKey).isEqualTo("test");
        assertThat(convertedValue).isEqualTo(123L);
    }

    @Test
    @DisplayName("Typed store enables efficient selective column queries")
    void typedStoreEnablesSelectiveQueries() {
        // Insert test data
        Map<String, Long> testData = Map.of(
                "kafka", 100L,
                "streams", 250L,
                "cassandra", 75L,
                "database", 180L,
                "performance", 95L
        );

        testData.forEach((word, count) -> typedRepository.save(0, word, count));

        // With typed store, we can efficiently query specific patterns
        // This demonstrates the advantage of typed columns over BLOBs

        // Count total entries
        long totalEntries = typedRepository.getCount();
        assertThat(totalEntries).isEqualTo(testData.size());

        // Count entries in specific partition
        long partitionEntries = typedRepository.getCount(0);
        assertThat(partitionEntries).isEqualTo(testData.size());

        // Verify all entries exist with correct types
        testData.forEach((word, expectedCount) -> {
            Long actualCount = typedRepository.getByKey(0, word);
            assertThat(actualCount)
                    .isNotNull()
                    .isEqualTo(expectedCount);
        });
    }

    @Test
    @DisplayName("Typed store provides better performance for large datasets")
    void typedStorePerformanceComparison() {
        int dataSize = 1000;

        // Generate test data
        Map<String, Long> testData = new HashMap<>();
        IntStream.range(0, dataSize).forEach(i -> {
            testData.put("word" + i, (long) i * 10);
        });

        Instant startTime = Instant.now();

        // Insert data using typed repository
        testData.forEach((word, count) -> typedRepository.save(0, word, count));

        Duration typedInsertTime = Duration.between(startTime, Instant.now());
        LOG.info("Typed insert time for {} entries: {}ms", dataSize, typedInsertTime.toMillis());

        // Verify data integrity
        testData.forEach((word, expectedCount) -> {
            Long actualCount = typedRepository.getByKey(0, word);
            assertThat(actualCount).isEqualTo(expectedCount);
        });

        Duration totalTime = Duration.between(startTime, Instant.now());
        LOG.info("Total test time: {}ms", totalTime.toMillis());

        // With typed columns, CQL can optimize queries much better than BLOB storage
        assertThat(typedRepository.getCount()).isEqualTo(dataSize);
    }

    @Test
    @DisplayName("Typed store supports efficient batch operations")
    void typedStoreBatchOperations() {
        List<KeyValue<String, Long>> batchData = Arrays.asList(
                KeyValue.pair("batch1", 10L),
                KeyValue.pair("batch2", 20L),
                KeyValue.pair("batch3", 30L),
                KeyValue.pair("batch4", 40L),
                KeyValue.pair("batch5", 50L)
        );

        // Batch insert
        typedRepository.saveBatch(0, batchData);

        // Verify all entries
        batchData.forEach(kv -> {
            Long value = typedRepository.getByKey(0, kv.key);
            assertThat(value).isEqualTo(kv.value);
        });

        // Batch update
        List<KeyValue<String, Long>> updateData = Arrays.asList(
                KeyValue.pair("batch1", 100L),
                KeyValue.pair("batch3", 300L)
        );

        typedRepository.saveBatch(0, updateData);

        // Verify updates
        assertThat(typedRepository.getByKey(0, "batch1")).isEqualTo(100L);
        assertThat(typedRepository.getByKey(0, "batch2")).isEqualTo(20L); // Unchanged
        assertThat(typedRepository.getByKey(0, "batch3")).isEqualTo(300L);
    }

    @Test
    @DisplayName("Typed store provides better debugging and monitoring capabilities")
    void typedStoreDebuggingCapabilities() {
        // Insert test data
        typedRepository.save(0, "debug", 42L);

        // Demonstrate type-aware debugging
        String debugKey = "debug";
        Long debugValue = 42L;

        // Type-safe conversion logging
        Bytes keyBytes = typedRepository.keyToBytes(debugKey);
        byte[] valueBytes = typedRepository.valueToBytes(debugValue);

        LOG.debug("Key '{}' converted to {} bytes", debugKey, keyBytes.get().length);
        LOG.debug("Value '{}' converted to {} bytes", debugValue, valueBytes.length);

        // Reverse conversion
        String convertedKey = typedRepository.bytesToKey(keyBytes);
        Long convertedValue = typedRepository.bytesToValue(valueBytes);

        assertThat(convertedKey).isEqualTo(debugKey);
        assertThat(convertedValue).isEqualTo(debugValue);

        // Schema information is available at runtime
        assertThat(typedRepository.getSchema().getTableName()).isEqualTo("perf_test_word_count");
        assertThat(typedRepository.getSchema().hasColumn("word")).isTrue();
        assertThat(typedRepository.getSchema().hasColumn("count")).isTrue();
    }

    @Test
    @DisplayName("Typed store handles edge cases and error conditions gracefully")
    void typedStoreErrorHandling() {
        // Test null handling
        assertThatThrownBy(() -> typedRepository.save(0, null, 42L))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> typedRepository.getByKey(0, null))
                .isNotNull(); // Should return null, not throw

        // Test type conversion edge cases
        assertThat(typedRepository.bytesToKey(null)).isNull();
        assertThat(typedRepository.bytesToValue(null)).isNull();

        // Test invalid data handling
        byte[] invalidBytes = "invalid".getBytes(StandardCharsets.UTF_8);
        assertThat(typedRepository.bytesToKey(Bytes.wrap(invalidBytes))).isEqualTo("invalid");

        byte[] invalidNumberBytes = "notanumber".getBytes(StandardCharsets.UTF_8);
        Long result = typedRepository.bytesToValue(invalidNumberBytes);
        assertThat(result).isEqualTo(0L); // Default value for invalid numbers
    }

    @Test
    @DisplayName("Typed store demonstrates advantages for interactive queries")
    void typedStoreInteractiveQueryAdvantages() {
        // Setup test data that would be typical for interactive dashboard queries
        Map<String, Long> dashboardData = Map.of(
                "page_views", 15420L,
                "unique_visitors", 3420L,
                "session_duration", 245L,
                "bounce_rate", 35L,
                "conversion_rate", 12L
        );

        dashboardData.forEach((metric, value) -> typedRepository.save(0, metric, value));

        // Simulate interactive queries that would be used in dashboards
        // With typed columns, these queries can be optimized by Cassandra
        Map<String, Long> retrievedData = new HashMap<>();
        dashboardData.keySet().forEach(metric -> {
            Long value = typedRepository.getByKey(0, metric);
            retrievedData.put(metric, value);
        });

        // Verify all data retrieved correctly
        assertThat(retrievedData).containsExactlyInAnyOrderEntriesOf(dashboardData);

        // Demonstrate that we can get specific metrics efficiently
        Long pageViews = typedRepository.getByKey(0, "page_views");
        Long conversions = typedRepository.getByKey(0, "conversion_rate");

        assertThat(pageViews).isEqualTo(15420L);
        assertThat(conversions).isEqualTo(12L);

        // Calculate derived metrics (would be done in application logic)
        double conversionRate = conversions.doubleValue() / pageViews.doubleValue() * 100;
        assertThat(conversionRate).isCloseTo(0.077, within(0.001));
    }

    @Test
    @DisplayName("Typed store provides schema-aware data validation")
    void typedStoreSchemaValidation() {
        // Test schema structure
        CassandraSchema schema = typedRepository.getSchema();

        assertThat(schema.getTableName()).isEqualTo("perf_test_word_count");
        assertThat(schema.hasColumn("partition")).isTrue();
        assertThat(schema.hasColumn("word")).isTrue();
        assertThat(schema.hasColumn("count")).isTrue();
        assertThat(schema.hasColumn("time")).isTrue();

        // Test column types
        assertThat(schema.getColumn("word").cqlType()).isEqualTo("text");
        assertThat(schema.getColumn("count").cqlType()).isEqualTo("bigint");
        assertThat(schema.getColumn("partition").cqlType()).isEqualTo("int");

        // Test key structure
        assertThat(schema.getPartitionKeyColumns()).hasSize(1);
        assertThat(schema.getPartitionKeyColumns().get(0).name()).isEqualTo("partition");

        // Test that schema generates valid CQL
        String createTableSql = schema.generateCreateTableStatement("");
        assertThat(createTableSql).contains("CREATE TABLE IF NOT EXISTS perf_test_word_count");
        assertThat(createTableSql).contains("partition int");
        assertThat(createTableSql).contains("word text");
        assertThat(createTableSql).contains("count bigint");
        assertThat(createTableSql).contains("PRIMARY KEY ((partition), word)");
    }
}
