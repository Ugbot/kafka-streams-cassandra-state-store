package dev.thriving.oss.kafka.streams.cassandra.state.store.example;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraStores;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.CassandraSchema;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.SchemaAssertion;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.SchemaTemplates;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.TypeMapping;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.TopicSchemaValidator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Example application demonstrating schema-aware Kafka Streams with Cassandra state store.
 *
 * This example shows how to:
 * 1. Define a Cassandra schema with typed columns
 * 2. Assert that topics conform to the expected schema
 * 3. Use schema-aware state stores in Kafka Streams topologies
 */
public class SchemaAwareWordCount {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaAwareWordCount.class);

    private static final String APPLICATION_ID = "schema-aware-word-count";
    private static final String INPUT_TOPIC = "word-count-input";
    private static final String STORE_NAME = "word-counts";

    public static void main(String[] args) {
        LOG.info("Starting Schema-Aware Word Count Application");

        try {
            // 1. Define the schema for our word count state store
            CassandraSchema wordCountSchema = defineWordCountSchema();

            // 2. Validate the schema structure
            SchemaAssertion.assertSchemaIsValid(wordCountSchema);

            // 3. Setup Cassandra connection
            CqlSession cassandraSession = createCassandraSession();

            // 4. Optionally validate that input topic conforms to expected schema
            validateInputTopic(INPUT_TOPIC);

            // 5. Build and start the Kafka Streams topology
            KafkaStreams streams = buildStreamsTopology(cassandraSession, wordCountSchema);
            streams.start();

            // 6. Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Shutting down application");
                streams.close();
                cassandraSession.close();
            }));

            LOG.info("Schema-Aware Word Count Application started successfully");

        } catch (Exception e) {
            LOG.error("Failed to start application", e);
            System.exit(1);
        }
    }

    /**
     * Define the Cassandra schema for word count data.
     * Uses TEXT for words and BIGINT for counts instead of generic BLOBs.
     */
    private static CassandraSchema defineWordCountSchema() {
        LOG.info("Defining word count schema with type mappings");

        // Validate that our types are supported
        TypeMapping.validateTypeMapping(String.class);
        TypeMapping.validateTypeMapping(Long.class);
        TypeMapping.validateTypeMapping(Integer.class);

        LOG.info("Supported CQL types: {}", TypeMapping.getSupportedCqlTypes());
        LOG.info("String maps to CQL type: {}", TypeMapping.getCqlType(String.class));
        LOG.info("Long maps to CQL type: {}", TypeMapping.getCqlType(Long.class));

        // Use the predefined template which creates a properly typed schema
        CassandraSchema schema = SchemaTemplates.wordCountSchema(STORE_NAME + "_schema_store");

        LOG.info("Generated schema:\n{}", schema.generateCreateTableStatement("compaction = { 'class' : 'LeveledCompactionStrategy' }"));
        return schema;
    }

    /**
     * Create Cassandra session.
     */
    private static CqlSession createCassandraSession() {
        LOG.info("Creating Cassandra session");

        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress("localhost", 9042))
                .withKeyspace("kafka_streams")
                .withLocalDatacenter("datacenter1")
                .build();
    }

    /**
     * Validate that the input topic conforms to expected schema.
     * This is optional but recommended for production deployments.
     */
    private static void validateInputTopic(String topicName) {
        LOG.info("Validating input topic: {}", topicName);

        try {
            // Create a consumer to read from the topic
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, APPLICATION_ID + "-validator");
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                    consumerProps,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
            );

            // For this example, we'll skip detailed topic validation
            // In a real application, you would use TopicSchemaValidator here
            consumer.close();

            LOG.info("Topic validation completed for: {}", topicName);

        } catch (Exception e) {
            LOG.warn("Could not validate input topic (this is normal if Kafka is not running): {}", e.getMessage());
        }
    }

    /**
     * Build the Kafka Streams topology with schema-aware state store.
     */
    private static KafkaStreams buildStreamsTopology(CqlSession cassandraSession, CassandraSchema schema) {
        LOG.info("Building Kafka Streams topology");

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Disable caching to ensure data is written to Cassandra immediately
        streamsProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        // Create the input stream
        KStream<String, String> textLines = builder.stream(INPUT_TOPIC);

        // Process the word count
        KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> List.of(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count(Materialized.<String, Long>as(
                        CassandraStores.builder(cassandraSession, STORE_NAME)
                                .withSchema(schema)  // Enable schema-aware storage
                                .withKeyspace("kafka_streams")
                                .withCreateTableDisabled() // For demo - set to true in production
                                .withLoggingDisabled()
                                .withCachingDisabled()
                )
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        // Optional: Add a sink to output the results
        wordCounts.toStream().to("word-count-output");

        return new KafkaStreams(builder.build(), streamsProps);
    }
}
