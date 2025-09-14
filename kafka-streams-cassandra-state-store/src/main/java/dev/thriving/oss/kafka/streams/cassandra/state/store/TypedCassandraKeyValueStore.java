package dev.thriving.oss.kafka.streams.cassandra.state.store;

import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed.TypedCassandraKeyValueStoreRepository;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

/**
 * A schema-aware KeyValueStore implementation that works with typed data
 * instead of raw byte arrays. Uses a typed repository to handle data mapping.
 *
 * @param <K> the type of keys maintained by this store
 * @param <V> the type of values maintained by this store
 */
public class TypedCassandraKeyValueStore<K, V> extends AbstractCassandraStore implements KeyValueStore<Bytes, byte[]> {

    private final TypedCassandraKeyValueStoreRepository<K, V> typedRepository;
    private final boolean isCountAllEnabled;

    public TypedCassandraKeyValueStore(String name,
                                      TypedCassandraKeyValueStoreRepository<K, V> typedRepository,
                                      boolean isCountAllEnabled) {
        super(name);
        this.typedRepository = typedRepository;
        this.isCountAllEnabled = isCountAllEnabled;
    }

    @Override
    public void put(Bytes key, byte[] value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        K typedKey = typedRepository.bytesToKey(key);
        V typedValue = value != null ? typedRepository.bytesToValue(value) : null;

        typedRepository.save(partition, typedKey, typedValue);
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        byte[] existingValue = get(key);
        if (existingValue == null) {
            put(key, value);
        }
        return existingValue;
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        // Convert bytes to typed objects
        List<KeyValue<K, V>> typedEntries = entries.stream()
                .map(entry -> {
                    K typedKey = typedRepository.bytesToKey(entry.key);
                    V typedValue = entry.value != null ? typedRepository.bytesToValue(entry.value) : null;
                    return KeyValue.pair(typedKey, typedValue);
                })
                .toList();

        typedRepository.saveBatch(partition, typedEntries);
    }

    @Override
    public byte[] delete(Bytes key) {
        byte[] existingValue = get(key);
        if (existingValue != null) {
            K typedKey = typedRepository.bytesToKey(key);
            typedRepository.delete(partition, typedKey);
        }
        return existingValue;
    }

    @Override
    public byte[] get(Bytes key) {
        if (key == null) {
            return null;
        }

        K typedKey = typedRepository.bytesToKey(key);
        V typedValue = typedRepository.getByKey(partition, typedKey);

        return typedValue != null ? typedRepository.valueToBytes(typedValue) : null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        K typedFrom = from != null ? typedRepository.bytesToKey(from) : null;
        K typedTo = to != null ? typedRepository.bytesToKey(to) : null;

        KeyValueIterator<K, V> typedIterator = typedRepository.getForRange(partition, typedFrom, typedTo, true, false);

        // Convert back to bytes
        return new TypedToBytesIterator<>(typedIterator, typedRepository);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(Bytes from, Bytes to) {
        K typedFrom = from != null ? typedRepository.bytesToKey(from) : null;
        K typedTo = to != null ? typedRepository.bytesToKey(to) : null;

        KeyValueIterator<K, V> typedIterator = typedRepository.getForRange(partition, typedFrom, typedTo, false, false);

        // Convert back to bytes
        return new TypedToBytesIterator<>(typedIterator, typedRepository);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        KeyValueIterator<K, V> typedIterator = typedRepository.getAll(partition, true);
        return new TypedToBytesIterator<>(typedIterator, typedRepository);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        KeyValueIterator<K, V> typedIterator = typedRepository.getAll(partition, false);
        return new TypedToBytesIterator<>(typedIterator, typedRepository);
    }

    @Override
    public long approximateNumEntries() {
        if (isCountAllEnabled) {
            return typedRepository.getCount();
        }
        // Return -1 to indicate that counting is not enabled or not supported
        return -1;
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    /**
     * Iterator that converts typed data back to byte arrays for Kafka Streams compatibility.
     */
    private static class TypedToBytesIterator<K, V> implements KeyValueIterator<Bytes, byte[]> {
        private final KeyValueIterator<K, V> typedIterator;
        private final TypedCassandraKeyValueStoreRepository<K, V> repository;

        public TypedToBytesIterator(KeyValueIterator<K, V> typedIterator,
                                   TypedCassandraKeyValueStoreRepository<K, V> repository) {
            this.typedIterator = typedIterator;
            this.repository = repository;
        }

        @Override
        public boolean hasNext() {
            return typedIterator.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            KeyValue<K, V> typedEntry = typedIterator.next();
            Bytes keyBytes = repository.keyToBytes(typedEntry.key);
            byte[] valueBytes = typedEntry.value != null ? repository.valueToBytes(typedEntry.value) : null;
            return KeyValue.pair(keyBytes, valueBytes);
        }

        @Override
        public void close() {
            typedIterator.close();
        }

        @Override
        public Bytes peekNextKey() {
            K typedKey = typedIterator.peekNextKey();
            return repository.keyToBytes(typedKey);
        }
    }
}

