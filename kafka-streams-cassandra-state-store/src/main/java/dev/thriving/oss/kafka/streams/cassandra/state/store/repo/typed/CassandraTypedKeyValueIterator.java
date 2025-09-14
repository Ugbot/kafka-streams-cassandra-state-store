package dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed;

import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * Iterator that converts Cassandra Row objects to typed KeyValue pairs.
 * Similar to TypedKeyValueIterator but works directly with Cassandra Row objects.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class CassandraTypedKeyValueIterator<K, V> implements KeyValueIterator<K, V> {

    private final Iterator<Row> rowIterator;
    private final Function<Row, Map<String, Object>> rowToMapFunction;
    private final Function<Map<String, Object>, K> mapToKeyFunction;
    private final Function<Map<String, Object>, V> mapToValueFunction;

    public CassandraTypedKeyValueIterator(Iterator<Row> rowIterator,
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
        // Cassandra Row iterators don't need explicit closing
        // The underlying ResultSet will be closed by Cassandra
    }

    @Override
    public K peekNextKey() {
        // Cassandra iterators don't support peeking ahead
        // This would require caching the next result
        throw new UnsupportedOperationException("peekNextKey is not supported by Cassandra iterators");
    }
}

