package dev.thriving.oss.kafka.streams.cassandra.state.store.schema;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static dev.thriving.oss.kafka.streams.cassandra.state.store.schema.SchemaValidation.ValidationResult;

/**
 * Provides assertion methods for validating Kafka topic schemas.
 * Throws exceptions when validation fails, making it suitable for use in
 * application startup code to ensure data compatibility.
 */
public class SchemaAssertion {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaAssertion.class);

    /**
     * Asserts that a topic conforms to the expected schema.
     * Throws an exception if validation fails.
     *
     * @param consumer Kafka consumer for reading topic data
     * @param keyDeserializer deserializer for record keys
     * @param valueDeserializer deserializer for record values
     * @param topicName name of the topic to validate
     * @param schema expected Cassandra schema
     * @throws SchemaValidationException if the topic does not conform to the schema
     */
    public static void assertTopicConformsToSchema(Consumer<byte[], byte[]> consumer,
                                                   Deserializer<?> keyDeserializer,
                                                   Deserializer<?> valueDeserializer,
                                                   String topicName,
                                                   CassandraSchema schema) {
        assertTopicConformsToSchema(consumer, keyDeserializer, valueDeserializer,
                                   topicName, schema, 100, Duration.ofSeconds(30));
    }

    /**
     * Asserts that a topic conforms to the expected schema.
     * Throws an exception if validation fails.
     *
     * @param consumer Kafka consumer for reading topic data
     * @param keyDeserializer deserializer for record keys
     * @param valueDeserializer deserializer for record values
     * @param topicName name of the topic to validate
     * @param schema expected Cassandra schema
     * @param sampleSize number of records to sample for validation
     * @param timeout maximum time to wait for records
     * @throws SchemaValidationException if the topic does not conform to the schema
     */
    public static void assertTopicConformsToSchema(Consumer<byte[], byte[]> consumer,
                                                   Deserializer<?> keyDeserializer,
                                                   Deserializer<?> valueDeserializer,
                                                   String topicName,
                                                   CassandraSchema schema,
                                                   int sampleSize,
                                                   Duration timeout) {
        LOG.info("Asserting topic '{}' conforms to schema '{}'", topicName, schema.getTableName());

        TopicSchemaValidator validator = new TopicSchemaValidator(consumer, keyDeserializer, valueDeserializer);
        ValidationResult result = validator.validateTopic(topicName, schema, sampleSize, timeout);

        if (!result.valid()) {
            String errorMessage = String.format(
                "Topic '%s' does not conform to schema '%s': %s",
                topicName, schema.getTableName(), result.message());

            LOG.error(errorMessage);
            for (String error : result.errors()) {
                LOG.error("Validation error: {}", error);
            }

            throw new SchemaValidationException(errorMessage, result.errors());
        }

        LOG.info("✓ Topic '{}' successfully validated against schema '{}'",
                topicName, schema.getTableName());
    }

    /**
     * Asserts that a Cassandra schema is structurally valid.
     * Performs static analysis without requiring topic data.
     *
     * @param schema the schema to validate
     * @throws SchemaValidationException if the schema is invalid
     */
    public static void assertSchemaIsValid(CassandraSchema schema) {
        LOG.info("Asserting schema '{}' is structurally valid", schema.getTableName());

        ValidationResult result = TopicSchemaValidator.validateSchemaCompatibility(schema);

        if (!result.valid()) {
            String errorMessage = String.format(
                "Schema '%s' is invalid: %s",
                schema.getTableName(), result.message());

            LOG.error(errorMessage);
            throw new SchemaValidationException(errorMessage, result.errors());
        }

        LOG.info("✓ Schema '{}' is structurally valid", schema.getTableName());
    }

    /**
     * Convenience method for asserting schema compatibility during application startup.
     * This method can be called early in the application lifecycle to fail fast
     * if there are schema mismatches.
     */
    public static void assertSchemasCompatible(CassandraSchema sourceSchema,
                                             CassandraSchema targetSchema) {
        // For now, perform basic compatibility checks
        // In a full implementation, this could include more sophisticated
        // schema evolution compatibility checks

        if (!sourceSchema.getTableName().equals(targetSchema.getTableName())) {
            throw new SchemaValidationException(
                "Schema table names do not match: '" + sourceSchema.getTableName() +
                "' vs '" + targetSchema.getTableName() + "'", java.util.Collections.emptyList());
        }

        // Check that all required columns exist
        for (ColumnDefinition targetColumn : targetSchema.getColumns()) {
            ColumnDefinition sourceColumn = sourceSchema.getColumn(targetColumn.name());
            if (sourceColumn == null) {
                throw new SchemaValidationException(
                    "Required column '" + targetColumn.name() + "' missing from source schema",
                    java.util.Collections.singletonList("Missing column: " + targetColumn.name()));
            }

            // Check type compatibility (basic check)
            if (!sourceColumn.cqlType().equals(targetColumn.cqlType())) {
                throw new SchemaValidationException(
                    "Column '" + targetColumn.name() + "' type mismatch: '" +
                    sourceColumn.cqlType() + "' vs '" + targetColumn.cqlType() + "'",
                    java.util.Collections.singletonList("Type mismatch for column: " + targetColumn.name()));
            }
        }

        LOG.info("✓ Schemas '{}' are compatible", sourceSchema.getTableName());
    }

    /**
     * Exception thrown when schema validation fails.
     */
    public static class SchemaValidationException extends RuntimeException {
        private final java.util.List<String> validationErrors;

        public SchemaValidationException(String message, java.util.List<String> validationErrors) {
            super(message);
            this.validationErrors = java.util.List.copyOf(validationErrors);
        }

        public java.util.List<String> getValidationErrors() {
            return validationErrors;
        }
    }
}
