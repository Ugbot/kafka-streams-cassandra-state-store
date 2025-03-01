package dev.thriving.oss.kafka.streams.cassandra.state.store.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraKeyValueIterator;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class GlobalCassandraKeyValueStoreRepository extends AbstractCassandraKeyValueStoreRepository<ByteBuffer> implements CassandraKeyValueStoreRepository {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalCassandraKeyValueStoreRepository.class);

    private PreparedStatement insert;
    private PreparedStatement deleteByKey;
    private PreparedStatement selectByKey;
    private PreparedStatement selectAll;
    private PreparedStatement selectCountAll;

    public GlobalCassandraKeyValueStoreRepository(CqlSession session,
                                                  String tableName,
                                                  boolean createTable,
                                                  String tableOptions,
                                                  String ddlExecutionProfile,
                                                  String dmlExecutionProfile) {
        super(session, tableName, createTable, tableOptions, ddlExecutionProfile, dmlExecutionProfile);
    }

    @Override
    protected String buildCreateTableQuery(String tableName, String tableOptions) {
        return """
                CREATE TABLE IF NOT EXISTS %s (
                    key blob,
                    time timestamp,
                    value blob,
                    PRIMARY KEY (key)
                ) %s
                """.formatted(tableName, tableOptions.isBlank() ? "" : "WITH " + tableOptions);
    }

    @Override
    protected void initPreparedStatements(String tableName) {
        insert = session.prepare("INSERT INTO " + tableName + " (key, time, value) VALUES (?, ?, ?)");
        deleteByKey = session.prepare("DELETE FROM " + tableName + " WHERE key=?");
        selectByKey = session.prepare("SELECT value FROM " + tableName + " WHERE key=?");
        selectAll = session.prepare("SELECT key, value FROM " + tableName);
        selectCountAll = session.prepare("SELECT COUNT(*) FROM " + tableName);
    }

    @Override
    public byte[] getByKey(int partition, Bytes key) {
        BoundStatement stmt = selectByKey.bind(ByteBuffer.wrap(key.get()));
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        ResultSet rs = session.execute(stmt);
        Row result = rs.one();
        if (result == null) {
            return null;
        }
        ByteBuffer byteBuffer = result.getByteBuffer(0);
        return byteBuffer == null ? null : byteBuffer.array();
    }

    @Override
    public void save(int partition, Bytes key, byte[] value) {
        BoundStatement stmt = insert.bind(ByteBuffer.wrap(key.get()), Instant.now(), ByteBuffer.wrap(value));
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        session.execute(stmt);
    }

    @Override
    public void saveBatch(int partition, List<KeyValue<Bytes, byte[]>> entries) {
        List<BatchableStatement<?>> inserts = new ArrayList<>();
        entries.forEach(it -> {
            inserts.add(insert.bind(ByteBuffer.wrap(it.key.get()), Instant.now(), ByteBuffer.wrap(it.value)));
        });
        BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.LOGGED);
        batch.addAll(inserts);
        if (dmlExecutionProfile != null) {
            batch = batch.setExecutionProfileName(ddlExecutionProfile);
        }
        session.execute(batch);
    }

    @Override
    public void delete(int partition, Bytes key) {
        BoundStatement stmt = deleteByKey.bind(ByteBuffer.wrap(key.get()));
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        session.execute(stmt);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> getAll(int partition, boolean forward) {
        if (!forward) {
            // Table uses `PRIMARY KEY (key)` and thus there's no clustering key to order by
            throw new UnsupportedOperationException("Getting all events in reverse order is not supported by globalKeyValueStore");
        }
        BoundStatement stmt = selectAll.bind();
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        ResultSet rs = session.execute(stmt);
        return new CassandraKeyValueIterator(rs.iterator());
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> getForRange(int partition, Bytes from, Bytes to, boolean forward, boolean toInclusive) {
        throw new UnsupportedOperationException("Range querys are not supported by globalKeyValueStore");
    }

    @Override
    public long getCount(int partition) {
        BoundStatement stmt = selectCountAll.bind();
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        return executeSelectCount(stmt);
    }

}
