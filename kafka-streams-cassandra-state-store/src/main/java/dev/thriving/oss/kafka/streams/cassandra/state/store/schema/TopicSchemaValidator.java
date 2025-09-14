package dev.thriving.oss.kafka.streams.cassandra.state.store.schema;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static dev.thriving.oss.kafka.streams.cassandra.state.store.schema.SchemaValidation.ValidationResult;

/**
 * Utility class for validating that Kafka topics conform to expected Cassandra schemas.
 * Provides methods to sample topic data and validate it against a schema definition.
 */
public class TopicSchemaValidator {

    private static final Logger LOG = LoggerFactory.getLogger(TopicSchemaValidator.class);

    private final Consumer<byte[], byte[]> consumer;
    private final Deserializer<?> keyDeserializer;
    private final Deserializer<?> valueDeserializer;

    public TopicSchemaValidator(Consumer<byte[], byte[]> consumer,
                               Deserializer<?> keyDeserializer,
                               Deserializer<?> valueDeserializer) {
        this.consumer = consumer;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    /**
     * Validates that a topic conforms to the expected schema by sampling records.
     *
     * @param topicName the name of the topic to validate
     * @param schema the expected Cassandra schema
     * @param sampleSize the number of records to sample (default: 100)
     * @return ValidationResult with details about the validation
     */
    public ValidationResult validateTopic(String topicName, CassandraSchema schema, int sampleSize) {
        return validateTopic(topicName, schema, sampleSize, Duration.ofSeconds(30));
    }

    /**
     * Validates that a topic conforms to the expected schema by sampling records.
     *
     * @param topicName the name of the topic to validate
     * @param schema the expected Cassandra schema
     * @param sampleSize the number of records to sample
     * @param timeout maximum time to wait for records
     * @return ValidationResult with details about the validation
     */
    public ValidationResult validateTopic(String topicName, CassandraSchema schema,
                                         int sampleSize, Duration timeout) {
        LOG.info("Starting topic schema validation for topic: {} with sample size: {}", topicName, sampleSize);

        try {
            // Get topic partitions
            List<TopicPartition> partitions = getTopicPartitions(topicName);
            if (partitions.isEmpty()) {
                return ValidationResult.failure("Topic '" + topicName + "' has no partitions or does not exist");
            }

            // Sample records from each partition
            List<ConsumerRecord<byte[], byte[]>> sampledRecords = sampleRecords(partitions, sampleSize, timeout);

            if (sampledRecords.isEmpty()) {
                LOG.warn("No records found in topic '{}' within timeout {}", topicName, timeout);
                return ValidationResult.success("Topic exists but contains no records to validate");
            }

            // Validate sampled records against schema
            return validateRecords(sampledRecords, schema);

        } catch (Exception e) {
            LOG.error("Error during topic validation for '{}': {}", topicName, e.getMessage(), e);
            return ValidationResult.failure("Validation failed due to error: " + e.getMessage());
        }
    }

    /**
     * Asynchronously validates a topic schema.
     */
    public CompletableFuture<ValidationResult> validateTopicAsync(String topicName,
                                                                  CassandraSchema schema,
                                                                  int sampleSize) {
        return CompletableFuture.supplyAsync(() ->
            validateTopic(topicName, schema, sampleSize));
    }

    /**
     * Gets all partitions for a topic.
     */
    private List<TopicPartition> getTopicPartitions(String topicName) {
        Map<String, List<Integer>> partitions = consumer.partitionsFor(topicName)
                .stream()
                .collect(HashMap::new,
                        (map, partition) -> map.computeIfAbsent(topicName, k -> new ArrayList<>())
                                .add(partition.partition()),
                        HashMap::putAll);

        return partitions.getOrDefault(topicName, Collections.emptyList())
                .stream()
                .map(partition -> new TopicPartition(topicName, partition))
                .toList();
    }

    /**
     * Samples records from topic partitions.
     */
    private List<ConsumerRecord<byte[], byte[]>> sampleRecords(List<TopicPartition> partitions,
                                                               int sampleSize,
                                                               Duration timeout) {
        List<ConsumerRecord<byte[], byte[]>> sampledRecords = new ArrayList<>();

        // Seek to end to get latest records
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);

        // Calculate how many records to get from each partition
        int recordsPerPartition = Math.max(1, sampleSize / partitions.size());

        // Seek back from end to get recent records
        for (TopicPartition partition : partitions) {
            long endOffset = consumer.position(partition);
            long startOffset = Math.max(0, endOffset - recordsPerPartition);
            consumer.seek(partition, startOffset);
        }

        // Poll for records
        long startTime = System.currentTimeMillis();
        long timeoutMs = timeout.toMillis();

        while (sampledRecords.size() < sampleSize &&
               (System.currentTimeMillis() - startTime) < timeoutMs) {

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                sampledRecords.add(record);
                if (sampledRecords.size() >= sampleSize) {
                    break;
                }
            }
        }

        LOG.debug("Sampled {} records from topic", sampledRecords.size());
        return sampledRecords;
    }

    /**
     * Validates a list of records against the schema.
     */
    private ValidationResult validateRecords(List<ConsumerRecord<byte[], byte[]>> records,
                                            CassandraSchema schema) {
        ValidationResult.Builder result = ValidationResult.builder();
        int validRecords = 0;
        int invalidRecords = 0;

        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<byte[], byte[]> record = records.get(i);
            ValidationResult recordResult = SchemaValidation.validateRecord(
                    record, keyDeserializer, valueDeserializer, schema);

            if (recordResult.valid()) {
                validRecords++;
            } else {
                invalidRecords++;
                result.addError(String.format("Record %d (offset %d): %s",
                        i, record.offset(), String.join("; ", recordResult.errors())));
            }
        }

        if (invalidRecords == 0) {
            result.setMessage(String.format("All %d sampled records conform to schema", records.size()));
        } else {
            result.setMessage(String.format("Found %d invalid records out of %d sampled (%.1f%% valid)",
                    invalidRecords, records.size(),
                    (validRecords * 100.0) / records.size()));
        }

        return result.build();
    }

    /**
     * Validates schema compatibility without sampling data.
     * Performs static analysis of the schema structure.
     */
    public static ValidationResult validateSchemaCompatibility(CassandraSchema schema) {
        ValidationResult.Builder result = ValidationResult.builder();

        try {
            // Generate CREATE TABLE statement to validate syntax
            String createStatement = schema.generateCreateTableStatement("");
            LOG.debug("Generated CREATE TABLE statement: {}", createStatement);

            // Additional validation rules can be added here
            // For example:
            // - Check for reserved CQL keywords
            // - Validate column type compatibility
            // - Check partition key distribution

            result.setMessage("Schema structure is valid");
            return result.build();

        } catch (Exception e) {
            result.addError("Schema validation failed: " + e.getMessage());
            return result.build();
        }
    }

    /**
     * Creates a human-readable schema compatibility report.
     */
    public static String generateCompatibilityReport(CassandraSchema schema) {
        String columnDetails = schema.getColumns().stream()
                .map(column -> "  %-20s %-15s %-10s%n".formatted(
                        column.name(),
                        column.cqlType(),
                        getKeyType(column)))
                .reduce("", String::concat);

        String partitionKeys = schema.getPartitionKeyColumns().stream()
                .map(ColumnDefinition::name)
                .reduce((a, b) -> a + " " + b)
                .orElse("");

        String clusteringKeys = schema.getClusteringKeyColumns().stream()
                .map(ColumnDefinition::name)
                .reduce((a, b) -> a + " " + b)
                .orElse("");

        String regularColumns = schema.getRegularColumns().stream()
                .map(ColumnDefinition::name)
                .reduce((a, b) -> a + " " + b)
                .orElse("");

        return """
            === Cassandra Schema Compatibility Report ===

            Table: %s

            Columns:
            %s
            Partition Keys: %s
            Clustering Keys: %s

            Regular Columns: %s

            Generated CQL:
            %s
            """.formatted(
                    schema.getTableName(),
                    columnDetails,
                    partitionKeys,
                    clusteringKeys,
                    regularColumns,
                    schema.generateCreateTableStatement("compaction = { 'class' : 'LeveledCompactionStrategy' }")
            );
    }

    private static String getKeyType(ColumnDefinition column) {
        if (column.isPartitionKey()) {
            return "PARTITION";
        } else if (column.isClusteringKey()) {
            return "CLUSTERING";
        } else if (column.isPrimaryKey()) {
            return "PRIMARY";
        } else {
            return "REGULAR";
        }
    }
}
