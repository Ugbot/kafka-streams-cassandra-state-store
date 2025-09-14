package dev.thriving.oss.kafka.streams.cassandra.state.store.schema;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

/**
 * Provides type mapping functionality between Java types and CQL types.
 * Supports both built-in types and custom type mappings.
 */
public class TypeMapping {

    private static final Map<Class<?>, String> JAVA_TO_CQL_TYPE_MAP = new HashMap<>();
    private static final Map<String, Class<?>> CQL_TO_JAVA_TYPE_MAP = new HashMap<>();
    private static final Map<Class<?>, DataType> JAVA_TO_CASSANDRA_TYPE_MAP = new HashMap<>();

    static {
        // Initialize built-in type mappings
        initializeTypeMappings();
    }

    private static void initializeTypeMappings() {
        // Primitive types
        mapType(Boolean.class, "boolean", DataTypes.BOOLEAN);
        mapType(boolean.class, "boolean", DataTypes.BOOLEAN);
        mapType(Byte.class, "tinyint", DataTypes.TINYINT);
        mapType(byte.class, "tinyint", DataTypes.TINYINT);
        mapType(Short.class, "smallint", DataTypes.SMALLINT);
        mapType(short.class, "smallint", DataTypes.SMALLINT);
        mapType(Integer.class, "int", DataTypes.INT);
        mapType(int.class, "int", DataTypes.INT);
        mapType(Long.class, "bigint", DataTypes.BIGINT);
        mapType(long.class, "bigint", DataTypes.BIGINT);
        mapType(Float.class, "float", DataTypes.FLOAT);
        mapType(float.class, "float", DataTypes.FLOAT);
        mapType(Double.class, "double", DataTypes.DOUBLE);
        mapType(double.class, "double", DataTypes.DOUBLE);

        // String types
        mapType(String.class, "text", DataTypes.TEXT);

        // Date/Time types
        mapType(Instant.class, "timestamp", DataTypes.TIMESTAMP);
        mapType(LocalDate.class, "date", DataTypes.DATE);
        mapType(LocalTime.class, "time", DataTypes.TIME);

        // Binary types
        mapType(ByteBuffer.class, "blob", DataTypes.BLOB);
        mapType(byte[].class, "blob", DataTypes.BLOB);

        // UUID types
        mapType(UUID.class, "uuid", DataTypes.UUID);

        // Network types
        mapType(InetAddress.class, "inet", DataTypes.INET);

        // Big number types
        mapType(BigInteger.class, "varint", DataTypes.VARINT);
        mapType(BigDecimal.class, "decimal", DataTypes.DECIMAL);

        // Collection types (these need special handling)
        mapType(List.class, "list<text>", DataTypes.listOf(DataTypes.TEXT));
        mapType(Set.class, "set<text>", DataTypes.setOf(DataTypes.TEXT));
        mapType(Map.class, "map<text,text>", DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT));
    }

    private static void mapType(Class<?> javaType, String cqlType, DataType cassandraType) {
        JAVA_TO_CQL_TYPE_MAP.put(javaType, cqlType);
        CQL_TO_JAVA_TYPE_MAP.put(cqlType, javaType);
        JAVA_TO_CASSANDRA_TYPE_MAP.put(javaType, cassandraType);
    }

    /**
     * Gets the CQL type string for a Java class.
     */
    public static String getCqlType(Class<?> javaType) {
        return JAVA_TO_CQL_TYPE_MAP.get(javaType);
    }

    /**
     * Gets the Java class for a CQL type string.
     */
    public static Class<?> getJavaType(String cqlType) {
        return CQL_TO_JAVA_TYPE_MAP.get(cqlType.toLowerCase());
    }

    /**
     * Gets the Cassandra DataType for a Java class.
     */
    public static DataType getCassandraType(Class<?> javaType) {
        return JAVA_TO_CASSANDRA_TYPE_MAP.get(javaType);
    }

    /**
     * Checks if a Java type is supported for CQL mapping.
     */
    public static boolean isSupportedType(Class<?> javaType) {
        return JAVA_TO_CQL_TYPE_MAP.containsKey(javaType);
    }

    /**
     * Creates a complex CQL type string for collections.
     */
    public static String createListType(String elementType) {
        return "list<%s>".formatted(elementType);
    }

    /**
     * Creates a complex CQL type string for sets.
     */
    public static String createSetType(String elementType) {
        return "set<%s>".formatted(elementType);
    }

    /**
     * Creates a complex CQL type string for maps.
     */
    public static String createMapType(String keyType, String valueType) {
        return "map<%s,%s>".formatted(keyType, valueType);
    }

    /**
     * Registers a custom type mapping.
     */
    public static void registerCustomType(Class<?> javaType, String cqlType, DataType cassandraType) {
        mapType(javaType, cqlType, cassandraType);
    }

    /**
     * Gets all supported Java types.
     */
    public static Set<Class<?>> getSupportedJavaTypes() {
        return new HashSet<>(JAVA_TO_CQL_TYPE_MAP.keySet());
    }

    /**
     * Gets all supported CQL types.
     */
    public static Set<String> getSupportedCqlTypes() {
        return new HashSet<>(CQL_TO_JAVA_TYPE_MAP.keySet());
    }

    /**
     * Validates that a Java type can be mapped to CQL.
     * Throws an exception if the type is not supported.
     */
    public static void validateTypeMapping(Class<?> javaType) {
        if (!isSupportedType(javaType)) {
            String supportedTypes = getSupportedJavaTypes().stream()
                    .map(Class::getSimpleName)
                    .sorted()
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("none");

            throw new IllegalArgumentException("""
                Java type '%s' is not supported for CQL mapping.
                Supported types: [%s]
                """.formatted(javaType.getSimpleName(), supportedTypes));
        }
    }

    /**
     * Validates that a CQL type can be mapped to Java.
     * Throws an exception if the type is not supported.
     */
    public static void validateCqlType(String cqlType) {
        if (!CQL_TO_JAVA_TYPE_MAP.containsKey(cqlType.toLowerCase())) {
            String supportedTypes = getSupportedCqlTypes().stream()
                    .sorted()
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("none");

            throw new IllegalArgumentException("""
                CQL type '%s' is not supported for Java mapping.
                Supported types: [%s]
                """.formatted(cqlType, supportedTypes));
        }
    }
}
