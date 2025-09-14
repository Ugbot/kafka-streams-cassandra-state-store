package dev.thriving.oss.kafka.streams.cassandra.state.store.schema;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Provides schema validation functionality for Kafka topics.
 * Validates that incoming records conform to the expected Cassandra schema structure.
 */
public class SchemaValidation {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaValidation.class);

    /**
     * Validates that a ConsumerRecord conforms to the expected schema.
     * This is used during topic consumption to ensure data integrity.
     *
     * @param record the Kafka record to validate
     * @param keyDeserializer deserializer for the key
     * @param valueDeserializer deserializer for the value
     * @param schema the expected Cassandra schema
     * @return ValidationResult indicating success or failure with details
     */
    public static ValidationResult validateRecord(
            ConsumerRecord<byte[], byte[]> record,
            Deserializer<?> keyDeserializer,
            Deserializer<?> valueDeserializer,
            CassandraSchema schema) {

        try {
            // Deserialize key and value to validate structure
            Object key = null;
            Object value = null;

            if (record.key() != null) {
                key = keyDeserializer.deserialize(record.topic(), record.key());
            }

            if (record.value() != null) {
                value = valueDeserializer.deserialize(record.topic(), record.value());
            }

            // Perform schema validation
            return validateDataStructure(key, value, schema);

        } catch (Exception e) {
            LOG.warn("Schema validation failed during deserialization for record at offset {}: {}",
                    record.offset(), e.getMessage());
            return ValidationResult.failure("Deserialization failed: " + e.getMessage());
        }
    }

    /**
     * Validates the data structure against the schema.
     * This method can be used independently of Kafka records.
     */
    public static ValidationResult validateDataStructure(Object key, Object value, CassandraSchema schema) {
        ValidationResult.Builder result = ValidationResult.builder();

        // For now, we perform basic null checks and type compatibility
        // In a full implementation, this would include more sophisticated validation
        // based on the CQL types defined in the schema

        if (key == null && hasRequiredKeyColumns(schema)) {
            result.addError("Key is null but schema requires key columns");
        }

        if (value == null && hasRequiredValueColumns(schema)) {
            result.addError("Value is null but schema requires value columns");
        }

        // Add more sophisticated validation here based on CQL types
        // For example:
        // - Check if numeric types are within bounds
        // - Validate string lengths
        // - Check enum values
        // - Validate collection types

        return result.build();
    }

    /**
     * Checks if the schema has required key columns that cannot be null.
     */
    private static boolean hasRequiredKeyColumns(CassandraSchema schema) {
        // For Kafka Streams compatibility, partition key is typically required
        return !schema.getPartitionKeyColumns().isEmpty();
    }

    /**
     * Checks if the schema has required value columns that cannot be null.
     */
    private static boolean hasRequiredValueColumns(CassandraSchema schema) {
        // Regular columns (non-primary key) are typically required
        return !schema.getRegularColumns().isEmpty();
    }

    /**
     * Result of a schema validation operation.
     *
     * @param valid whether the validation passed
     * @param message descriptive message about the validation result
     * @param errors list of validation errors (empty if valid)
     */
    public record ValidationResult(
            boolean valid,
            String message,
            java.util.List<String> errors
    ) {

        public ValidationResult {
            errors = errors != null ? java.util.List.copyOf(errors) : java.util.List.of();
        }

        /**
         * Creates a successful validation result.
         */
        public static ValidationResult success() {
            return new ValidationResult(true, "Validation successful", java.util.List.of());
        }

        /**
         * Creates a successful validation result with a custom message.
         */
        public static ValidationResult success(String message) {
            return new ValidationResult(true, message, java.util.List.of());
        }

        /**
         * Creates a failed validation result.
         */
        public static ValidationResult failure(String message) {
            return new ValidationResult(false, message, java.util.List.of(message));
        }

        /**
         * Creates a failed validation result with multiple errors.
         */
        public static ValidationResult failure(java.util.List<String> errors) {
            return new ValidationResult(false, "Validation failed with " + errors.size() + " errors", errors);
        }

        /**
         * Builder for creating ValidationResult instances.
         */
        public record Builder(java.util.List<String> errors, String message) {

            public Builder() {
                this(new java.util.ArrayList<>(), "");
            }

            public Builder addError(String error) {
                var newErrors = new java.util.ArrayList<>(errors);
                newErrors.add(error);
                return new Builder(newErrors, message);
            }

            public Builder setMessage(String message) {
                return new Builder(errors, message);
            }

            public ValidationResult build() {
                if (errors.isEmpty()) {
                    return success(message.isEmpty() ? "Validation successful" : message);
                } else {
                    return failure(errors);
                }
            }
        }

        /**
         * Creates a new builder for constructing ValidationResult instances.
         */
        public static Builder builder() {
            return new Builder();
        }
    }
}
