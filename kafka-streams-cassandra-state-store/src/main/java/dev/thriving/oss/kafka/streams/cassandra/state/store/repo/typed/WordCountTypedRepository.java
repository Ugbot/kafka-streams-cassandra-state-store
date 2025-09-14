package dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.CassandraSchema;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.TypeMapping;
import org.apache.kafka.common.utils.Bytes;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Concrete implementation of a typed Cassandra repository for word count data.
 * Demonstrates how to map structured Java objects to CQL columns.
 */
public class WordCountTypedRepository extends TypedPartitionedCassandraKeyValueStoreRepository<String, Long> {

    public WordCountTypedRepository(CqlSession session,
                                   String tableName,
                                   CassandraSchema schema,
                                   boolean createTable,
                                   String tableOptions,
                                   String ddlExecutionProfile,
                                   String dmlExecutionProfile) {
        super(session, tableName, schema, createTable, tableOptions, ddlExecutionProfile, dmlExecutionProfile);
    }

    @Override
    public Map<String, Object> keyToColumnMap(String key) {
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("word", key);
        return columnMap;
    }

    @Override
    public Map<String, Object> valueToColumnMap(Long value) {
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("count", value);
        return columnMap;
    }

    @Override
    public String rowToKey(Map<String, Object> row) {
        return (String) row.get("word");
    }

    @Override
    public Long rowToValue(Map<String, Object> row) {
        Number count = (Number) row.get("count");
        return count != null ? count.longValue() : 0L;
    }

    @Override
    public Bytes keyToBytes(String key) {
        if (key == null) return null;
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        return Bytes.wrap(bytes);
    }

    @Override
    public String bytesToKey(Bytes bytes) {
        if (bytes == null) return null;
        return new String(bytes.get(), StandardCharsets.UTF_8);
    }

    @Override
    public byte[] valueToBytes(Long value) {
        if (value == null) return null;
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Long bytesToValue(byte[] bytes) {
        if (bytes == null) return null;
        String str = new String(bytes, StandardCharsets.UTF_8);
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}

