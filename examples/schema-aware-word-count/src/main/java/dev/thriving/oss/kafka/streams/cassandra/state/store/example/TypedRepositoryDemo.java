package dev.thriving.oss.kafka.streams.cassandra.state.store.example;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed.WordCountTypedRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.CassandraSchema;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.SchemaTemplates;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Demo class showing how to use the typed repository directly.
 * Demonstrates type-safe operations with structured data.
 */
public class TypedRepositoryDemo {

    private static final Logger LOG = LoggerFactory.getLogger(TypedRepositoryDemo.class);

    public static void main(String[] args) {
        LOG.info("Starting Typed Repository Demo");

        try (CqlSession session = createCassandraSession()) {
            // Define schema
            CassandraSchema schema = SchemaTemplates.wordCountSchema("demo_word_count");

            // Create typed repository
            WordCountTypedRepository repository = new WordCountTypedRepository(
                    session,
                    "demo_word_count",
                    schema,
                    true, // create table
                    "compaction = { 'class' : 'LeveledCompactionStrategy' }",
                    null, // ddl profile
                    null  // dml profile
            );

            // Demonstrate typed operations
            demonstrateTypedOperations(repository);

            LOG.info("Typed Repository Demo completed successfully");

        } catch (Exception e) {
            LOG.error("Demo failed", e);
        }
    }

    private static void demonstrateTypedOperations(WordCountTypedRepository repository) {
        LOG.info("=== Demonstrating Typed Repository Operations ===");

        // 1. Save individual entries with typed data
        LOG.info("1. Saving individual entries...");
        repository.save(0, "hello", 5L);
        repository.save(0, "world", 3L);
        repository.save(0, "kafka", 10L);

        // 2. Get values with type safety
        LOG.info("2. Retrieving values...");
        Long helloCount = repository.getByKey(0, "hello");
        Long worldCount = repository.getByKey(0, "world");
        Long kafkaCount = repository.getByKey(0, "kafka");

        LOG.info("hello: {}", helloCount);
        LOG.info("world: {}", worldCount);
        LOG.info("kafka: {}", kafkaCount);

        // 3. Batch save with typed data
        LOG.info("3. Batch saving entries...");
        List<KeyValue<String, Long>> batchEntries = Arrays.asList(
                KeyValue.pair("streams", 7L),
                KeyValue.pair("cassandra", 4L),
                KeyValue.pair("database", 2L)
        );
        repository.saveBatch(0, batchEntries);

        // 4. Update existing entries
        LOG.info("4. Updating existing entries...");
        repository.save(0, "hello", 8L); // Update hello count

        // 5. Get updated value
        Long updatedHelloCount = repository.getByKey(0, "hello");
        LOG.info("Updated hello count: {}", updatedHelloCount);

        // 6. Delete an entry
        LOG.info("5. Deleting an entry...");
        repository.delete(0, "world");

        Long deletedWorldCount = repository.getByKey(0, "world");
        LOG.info("world count after deletion: {}", deletedWorldCount);

        // 7. Get count statistics
        LOG.info("6. Getting count statistics...");
        long totalCount = repository.getCount();
        long partitionCount = repository.getCount(0);

        LOG.info("Total entries across all partitions: {}", totalCount);
        LOG.info("Entries in partition 0: {}", partitionCount);

        // 8. Demonstrate type conversion
        LOG.info("7. Demonstrating type conversions...");
        var keyBytes = repository.keyToBytes("test");
        var valueBytes = repository.valueToBytes(42L);

        String convertedKey = repository.bytesToKey(keyBytes);
        Long convertedValue = repository.bytesToValue(valueBytes);

        LOG.info("Key conversion: '{}' -> bytes -> '{}'", "test", convertedKey);
        LOG.info("Value conversion: '{}' -> bytes -> '{}'", 42L, convertedValue);

        LOG.info("=== Demo completed successfully ===");
    }

    private static CqlSession createCassandraSession() {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress("localhost", 9042))
                .withKeyspace("demo_keyspace")
                .withLocalDatacenter("datacenter1")
                .build();
    }
}

