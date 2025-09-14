package dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed;

import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * Iterator that converts CQL Row objects to typed KeyValue pairs.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class TypedKeyValueIterator<K, V> implements KeyValueIterator<K, V> {

    private final Iterator<Row> rowIterator;
    private final Function<Row, Map<String, Object>> rowToMapFunction;
    private final Function<Map<String, Object>, K> mapToKeyFunction;
    private final Function<Map<String, Object>, V> mapToValueFunction;

    public TypedKeyValueIterator(Iterator<Row> rowIterator,
                                Function<Row, Map<String, Object>> rowToMapFunction,
                                Function<Map<String, Object>, K> mapToKeyFunction,
                                Function<Map<String, Object>, V> mapToValueFunction) {
        this.rowIterator = rowIterator;
        this.rowToMapFunction = rowToMapFunction;
        this.mapToKeyFunction = mapToKeyFunction;
        this.mapToValueFunction = mapToValueFunction;
    }

    @Override
    public boolean hasNext() {
        return rowIterator.hasNext();
    }

    @Override
    public KeyValue<K, V> next() {
        Row row = rowIterator.next();
        Map<String, Object> rowMap = rowToMapFunction.apply(row);
        K key = mapToKeyFunction.apply(rowMap);
        V value = mapToValueFunction.apply(rowMap);
        return KeyValue.pair(key, value);
    }

    @Override
    public void close() {
        // No resources to close for CQL iterators
    }

    @Override
    public K peekNextKey() {
        // This would require peeking ahead in the iterator
        // For simplicity, we'll throw UnsupportedOperationException
        throw new UnsupportedOperationException("peekNextKey is not supported");
    }
}
