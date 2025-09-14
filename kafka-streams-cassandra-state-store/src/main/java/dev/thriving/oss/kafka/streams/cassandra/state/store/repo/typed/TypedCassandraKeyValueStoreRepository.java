package dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed;

import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.CassandraSchema;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.List;
import java.util.Map;

/**
 * Interface for typed Cassandra repositories that work with structured data
 * instead of raw byte arrays. Uses schema definitions to map data to/from CQL types.
 *
 * @param <K> the type of keys maintained by this repository
 * @param <V> the type of values maintained by this repository
 */
public interface TypedCassandraKeyValueStoreRepository<K, V> {

    /**
     * Gets a value by its key from the specified partition.
     */
    V getByKey(int partition, K key);

    /**
     * Saves a key-value pair to the specified partition.
     */
    void save(int partition, K key, V value);

    /**
     * Saves multiple key-value pairs in batch to the specified partition.
     */
    void saveBatch(int partition, List<KeyValue<K, V>> entries);

    /**
     * Deletes a key-value pair from the specified partition.
     */
    void delete(int partition, K key);

    /**
     * Gets all key-value pairs from the specified partition.
     *
     * @param partition the partition to query
     * @param forward whether to iterate in forward order
     * @return an iterator over all key-value pairs in the partition
     */
    KeyValueIterator<K, V> getAll(int partition, boolean forward);

    /**
     * Gets key-value pairs within a range from the specified partition.
     *
     * @param partition the partition to query
     * @param from the starting key (inclusive if not null)
     * @param to the ending key (inclusive/exclusive based on toInclusive)
     * @param forward whether to iterate in forward order
     * @param toInclusive whether the 'to' key should be inclusive
     * @return an iterator over key-value pairs in the specified range
     */
    KeyValueIterator<K, V> getForRange(int partition, K from, K to, boolean forward, boolean toInclusive);

    /**
     * Gets the total count of entries across all partitions.
     */
    long getCount();

    /**
     * Gets the count of entries in a specific partition.
     */
    long getCount(int partition);

    /**
     * Gets the schema definition for this repository.
     */
    CassandraSchema getSchema();

    /**
     * Converts a typed key to its byte representation for Kafka Streams compatibility.
     */
    default Bytes keyToBytes(K key) {
        // Default implementation - subclasses should override for proper serialization
        throw new UnsupportedOperationException("keyToBytes must be implemented by subclasses");
    }

    /**
     * Converts a byte array to a typed key for Kafka Streams compatibility.
     */
    default K bytesToKey(Bytes bytes) {
        // Default implementation - subclasses should override for proper deserialization
        throw new UnsupportedOperationException("bytesToKey must be implemented by subclasses");
    }

    /**
     * Converts a typed value to its byte representation for Kafka Streams compatibility.
     */
    default byte[] valueToBytes(V value) {
        // Default implementation - subclasses should override for proper serialization
        throw new UnsupportedOperationException("valueToBytes must be implemented by subclasses");
    }

    /**
     * Converts a byte array to a typed value for Kafka Streams compatibility.
     */
    default V bytesToValue(byte[] bytes) {
        // Default implementation - subclasses should override for proper deserialization
        throw new UnsupportedOperationException("bytesToValue must be implemented by subclasses");
    }

    /**
     * Converts a typed key to a map of column values for CQL insertion.
     * This is used internally by the repository implementation.
     */
    Map<String, Object> keyToColumnMap(K key);

    /**
     * Converts a typed value to a map of column values for CQL insertion.
     * This is used internally by the repository implementation.
     */
    Map<String, Object> valueToColumnMap(V value);

    /**
     * Converts a CQL result row to a typed key.
     * This is used internally by the repository implementation.
     */
    K rowToKey(Map<String, Object> row);

    /**
     * Converts a CQL result row to a typed value.
     * This is used internally by the repository implementation.
     */
    V rowToValue(Map<String, Object> row);
}
