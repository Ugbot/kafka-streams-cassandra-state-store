package dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Comparator;
import java.util.NoSuchElementException;

/**
 * A filtering iterator that wraps another KeyValueIterator and filters results
 * based on key range criteria.
 *
 * @param <K> the type of keys (must be Comparable)
 * @param <V> the type of values
 */
public class FilteringKeyValueIterator<K extends Comparable<K>, V> implements KeyValueIterator<K, V> {

    private final KeyValueIterator<K, V> wrappedIterator;
    private final K fromKey;
    private final K toKey;
    private final boolean toInclusive;
    private KeyValue<K, V> nextElement;
    private boolean nextElementValid = false;

    public FilteringKeyValueIterator(KeyValueIterator<K, V> wrappedIterator,
                                   K fromKey, K toKey, boolean toInclusive) {
        this.wrappedIterator = wrappedIterator;
        this.fromKey = fromKey;
        this.toKey = toKey;
        this.toInclusive = toInclusive;
    }

    @Override
    public boolean hasNext() {
        if (!nextElementValid) {
            advanceToNextValid();
        }
        return nextElementValid;
    }

    @Override
    public KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        KeyValue<K, V> result = nextElement;
        nextElementValid = false;
        return result;
    }

    @Override
    public void close() {
        wrappedIterator.close();
    }

    @Override
    public K peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nextElement.key;
    }

    private void advanceToNextValid() {
        while (wrappedIterator.hasNext()) {
            KeyValue<K, V> candidate = wrappedIterator.next();
            if (isInRange(candidate.key)) {
                nextElement = candidate;
                nextElementValid = true;
                return;
            }
        }
        nextElementValid = false;
    }

    private boolean isInRange(K key) {
        // Check lower bound
        if (fromKey != null && key.compareTo(fromKey) < 0) {
            return false;
        }

        // Check upper bound
        if (toKey != null) {
            int comparison = key.compareTo(toKey);
            if (toInclusive) {
                return comparison <= 0;
            } else {
                return comparison < 0;
            }
        }

        return true;
    }
}
