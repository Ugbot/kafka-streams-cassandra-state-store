package dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraKeyValueIterator;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.CassandraSchema;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Typed implementation of a partitioned Cassandra key-value store repository.
 * Works with structured data defined by a Cassandra schema instead of raw bytes.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public abstract class TypedPartitionedCassandraKeyValueStoreRepository<K, V>
        extends AbstractTypedCassandraRepository<K, V> {

    public TypedPartitionedCassandraKeyValueStoreRepository(CqlSession session,
                                                           String tableName,
                                                           CassandraSchema schema,
                                                           boolean createTable,
                                                           String tableOptions,
                                                           String ddlExecutionProfile,
                                                           String dmlExecutionProfile) {
        super(session, tableName, schema, createTable, tableOptions, ddlExecutionProfile, dmlExecutionProfile);
    }

    @Override
    protected void initPreparedStatements(String tableName) {
        insertStatement = session.prepare(buildInsertStatement(tableName));
        selectByKeyStatement = session.prepare(buildSelectByKeyStatement(tableName));
        deleteByKeyStatement = session.prepare(buildDeleteByKeyStatement(tableName));
        selectAllStatement = session.prepare(buildSelectAllStatement(tableName, false));
        selectAllReversedStatement = session.prepare(buildSelectAllStatement(tableName, true));
        selectCountStatement = session.prepare(buildSelectCountStatement(tableName, false));
        selectCountByPartitionStatement = session.prepare(buildSelectCountStatement(tableName, true));
    }

    @Override
    public V getByKey(int partition, K key) {
        BoundStatement stmt = bindKeyToStatement(selectByKeyStatement, key, partition);
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        ResultSet rs = session.execute(stmt);
        Row result = rs.one();
        if (result == null) {
            return null;
        } else {
            Map<String, Object> rowMap = rowToMap(result);
            return rowToValue(rowMap);
        }
    }

    @Override
    public void save(int partition, K key, V value) {
        BoundStatement stmt = bindInsertStatement(key, value, partition);
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        session.execute(stmt);
    }

    @Override
    public void saveBatch(int partition, List<KeyValue<K, V>> entries) {
        List<BoundStatement> inserts = new ArrayList<>();
        entries.forEach(entry -> {
            inserts.add(bindInsertStatement(entry.key, entry.value, partition));
        });

        // Execute batch
        var batch = com.datastax.oss.driver.api.core.cql.BatchStatement.newInstance(
                com.datastax.oss.driver.api.core.cql.DefaultBatchType.LOGGED);
        batch = batch.addAll(inserts);

        if (dmlExecutionProfile != null) {
            batch = batch.setExecutionProfileName(ddlExecutionProfile);
        }
        session.execute(batch);
    }

    @Override
    public void delete(int partition, K key) {
        BoundStatement stmt = bindKeyToStatement(deleteByKeyStatement, key, partition);
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        session.execute(stmt);
    }

    @Override
    public KeyValueIterator<K, V> getAll(int partition, boolean forward) {
        PreparedStatement statement = forward ? selectAllStatement : selectAllReversedStatement;
        BoundStatement stmt = statement.bind(partition);
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        ResultSet rs = session.execute(stmt);

        // Convert ResultSet to typed iterator
        return new CassandraTypedKeyValueIterator<>(rs.iterator(), this::rowToMap, this::rowToKey, this::rowToValue);
    }

    @Override
    public KeyValueIterator<K, V> getForRange(int partition, K from, K to, boolean forward, boolean toInclusive) {
        // For simplicity, we'll use the all query for now
        // In a full implementation, you'd want to create specific range queries
        // and proper filtering based on CQL WHERE clauses
        return getAll(partition, forward);
    }

    @Override
    public long getCount() {
        BoundStatement stmt = selectCountStatement.bind();
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        return executeSelectCount(stmt);
    }

    @Override
    public long getCount(int partition) {
        BoundStatement stmt = selectCountByPartitionStatement.bind(partition);
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        return executeSelectCount(stmt);
    }

    // Abstract methods that must be implemented by concrete subclasses

    @Override
    public abstract Bytes keyToBytes(K key);

    @Override
    public abstract K bytesToKey(Bytes bytes);

    @Override
    public abstract byte[] valueToBytes(V value);

    @Override
    public abstract V bytesToValue(byte[] bytes);

    @Override
    public abstract Map<String, Object> keyToColumnMap(K key);

    @Override
    public abstract Map<String, Object> valueToColumnMap(V value);

    @Override
    public abstract K rowToKey(Map<String, Object> row);

    @Override
    public abstract V rowToValue(Map<String, Object> row);
}
