package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.PartitionedCassandraKeyValueStoreRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.typed.WordCountTypedRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.CassandraSchema;
import dev.thriving.oss.kafka.streams.cassandra.state.store.schema.SchemaTemplates;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests demonstrating the advantages of typed schema-aware stores for interactive queries,
 * particularly for dashboard and real-time analytics scenarios.
 */
@Tag("integration")
public class InteractiveQueryAdvantageTest extends AbstractIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(InteractiveQueryAdvantageTest.class);

    private CqlSession session;
    private CassandraSchema wordCountSchema;
    private WordCountTypedRepository typedRepository;

    @BeforeEach
    void setUp() {
        session = initSession();
        wordCountSchema = SchemaTemplates.wordCountSchema("interactive_test_word_count");

        typedRepository = new WordCountTypedRepository(
                session,
                "interactive_test_word_count",
                wordCountSchema,
                true,
                "compaction = { 'class' : 'LeveledCompactionStrategy' }",
                null,
                null
        );
    }

    @Test
    @DisplayName("Interactive queries: Typed store enables efficient dashboard data retrieval")
    void typedStoreDashboardQueryEfficiency() {
        // Simulate dashboard metrics that would be frequently queried
        Map<String, Long> dashboardMetrics = Map.of(
                "total_page_views", 125430L,
                "unique_users", 89234L,
                "active_sessions", 3456L,
                "conversion_rate", 324L,
                "avg_session_time", 245L,
                "bounce_rate", 1234L,
                "error_rate", 89L,
                "cache_hit_rate", 9876L
        );

        // Insert dashboard data
        dashboardMetrics.forEach((metric, value) -> typedRepository.save(0, metric, value));

        Instant queryStart = Instant.now();

        // Simulate dashboard loading multiple metrics simultaneously
        // This is a common pattern in real-time dashboards
        Map<String, Long> retrievedMetrics = new HashMap<>();

        // Parallel queries (simulating dashboard loading)
        List<CompletableFuture<Void>> queryFutures = dashboardMetrics.keySet().stream()
                .map(metric -> CompletableFuture.runAsync(() -> {
                    Long value = typedRepository.getByKey(0, metric);
                    synchronized (retrievedMetrics) {
                        retrievedMetrics.put(metric, value);
                    }
                }))
                .toList();

        // Wait for all queries to complete
        CompletableFuture.allOf(queryFutures.toArray(new CompletableFuture[0]))
                .join();

        Duration queryTime = Duration.between(queryStart, Instant.now());

        LOG.info("Retrieved {} metrics in {}ms", dashboardMetrics.size(), queryTime.toMillis());
        assertThat(retrievedMetrics).containsExactlyInAnyOrderEntriesOf(dashboardMetrics);

        // Verify specific metrics for dashboard calculations
        Long pageViews = retrievedMetrics.get("total_page_views");
        Long conversions = retrievedMetrics.get("conversion_rate");

        if (pageViews != null && conversions != null) {
            double actualConversionRate = (double) conversions / pageViews * 100;
            LOG.info("Calculated conversion rate: {:.2f}%", actualConversionRate);
            assertThat(actualConversionRate).isGreaterThan(0).isLessThan(100);
        }
    }

    @Test
    @DisplayName("Interactive queries: Typed store supports efficient real-time updates")
    void typedStoreRealTimeUpdates() {
        String metricName = "real_time_counter";
        int updateCount = 100;

        Instant testStart = Instant.now();

        // Simulate real-time counter updates (like active user count)
        IntStream.range(0, updateCount).forEach(i -> {
            Long currentValue = typedRepository.getByKey(0, metricName);
            Long newValue = (currentValue != null ? currentValue : 0L) + 1;
            typedRepository.save(0, metricName, newValue);

            // Small delay to simulate real-time updates
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Duration testDuration = Duration.between(testStart, Instant.now());
        Long finalValue = typedRepository.getByKey(0, metricName);

        LOG.info("Performed {} real-time updates in {}ms", updateCount, testDuration.toMillis());
        assertThat(finalValue).isEqualTo((long) updateCount);

        // Calculate updates per second
        double updatesPerSecond = updateCount / (testDuration.toMillis() / 1000.0);
        LOG.info("Update rate: {:.1f} updates/second", updatesPerSecond);
        assertThat(updatesPerSecond).isGreaterThan(10); // Should be reasonably fast
    }

    @Test
    @DisplayName("Interactive queries: Typed store enables efficient range queries for analytics")
    void typedStoreAnalyticsRangeQueries() {
        // Simulate time-series analytics data
        Map<String, Long> timeSeriesData = new LinkedHashMap<>();
        for (int hour = 0; hour < 24; hour++) {
            String key = String.format("hour_%02d", hour);
            Long value = (long) (Math.random() * 1000 + 100); // Random traffic data
            timeSeriesData.put(key, value);
            typedRepository.save(0, key, value);
        }

        Instant queryStart = Instant.now();

        // Simulate analytics query: get traffic for business hours (9-17)
        Map<String, Long> businessHoursData = new HashMap<>();
        for (int hour = 9; hour <= 17; hour++) {
            String key = String.format("hour_%02d", hour);
            Long value = typedRepository.getByKey(0, key);
            businessHoursData.put(key, value);
        }

        Duration queryTime = Duration.between(queryStart, Instant.now());

        LOG.info("Retrieved {} business hours metrics in {}ms", businessHoursData.size(), queryTime.toMillis());

        // Calculate business hours statistics
        long totalBusinessTraffic = businessHoursData.values().stream().mapToLong(Long::longValue).sum();
        double avgBusinessTraffic = businessHoursData.values().stream().mapToLong(Long::longValue).average().orElse(0);

        LOG.info("Total business hours traffic: {}", totalBusinessTraffic);
        LOG.info("Average business hours traffic: {:.1f}", avgBusinessTraffic);

        assertThat(businessHoursData).hasSize(9); // 9 business hours
        assertThat(totalBusinessTraffic).isGreaterThan(0);
    }

    @Test
    @DisplayName("Interactive queries: Typed store supports efficient aggregation queries")
    void typedStoreAggregationQueries() {
        // Simulate product sales data for e-commerce dashboard
        Map<String, Long> productSales = Map.of(
                "product_A", 1250L,
                "product_B", 890L,
                "product_C", 2100L,
                "product_D", 675L,
                "product_E", 1540L,
                "product_F", 320L,
                "product_G", 980L,
                "product_H", 1675L
        );

        productSales.forEach((product, sales) -> typedRepository.save(0, product, sales));

        Instant aggregationStart = Instant.now();

        // Simulate dashboard aggregation: calculate sales statistics
        List<Long> salesFigures = productSales.keySet().stream()
                .map(product -> typedRepository.getByKey(0, product))
                .toList();

        long totalSales = salesFigures.stream().mapToLong(Long::longValue).sum();
        double avgSales = salesFigures.stream().mapToLong(Long::longValue).average().orElse(0);
        long maxSales = salesFigures.stream().mapToLong(Long::longValue).max().orElse(0);
        long minSales = salesFigures.stream().mapToLong(Long::longValue).min().orElse(0);

        Duration aggregationTime = Duration.between(aggregationStart, Instant.now());

        LOG.info("Calculated sales statistics in {}ms", aggregationTime.toMillis());
        LOG.info("Total sales: {}, Average: {:.1f}, Max: {}, Min: {}",
                totalSales, avgSales, maxSales, minSales);

        // Verify calculations
        assertThat(totalSales).isEqualTo(productSales.values().stream().mapToLong(Long::longValue).sum());
        assertThat(maxSales).isEqualTo(productSales.values().stream().mapToLong(Long::longValue).max().orElse(0));
        assertThat(minSales).isEqualTo(productSales.values().stream().mapToLong(Long::longValue).min().orElse(0));
    }

    @Test
    @DisplayName("Interactive queries: Typed store enables efficient data export for reporting")
    void typedStoreDataExportEfficiency() {
        // Simulate large dataset for reporting
        int dataSize = 500;
        Map<String, Long> reportData = new HashMap<>();

        // Generate report data
        IntStream.range(0, dataSize).forEach(i -> {
            String key = String.format("report_metric_%03d", i);
            Long value = (long) (Math.random() * 10000);
            reportData.put(key, value);
            typedRepository.save(0, key, value);
        });

        Instant exportStart = Instant.now();

        // Simulate data export for reporting (common dashboard operation)
        Map<String, Long> exportedData = new HashMap<>();
        reportData.keySet().forEach(key -> {
            Long value = typedRepository.getByKey(0, key);
            exportedData.put(key, value);
        });

        Duration exportTime = Duration.between(exportStart, Instant.now());

        LOG.info("Exported {} data points in {}ms", dataSize, exportTime.toMillis());

        // Calculate export performance
        double exportRate = dataSize / (exportTime.toMillis() / 1000.0);
        LOG.info("Export rate: {:.1f} records/second", exportRate);

        // Verify data integrity
        assertThat(exportedData).containsExactlyInAnyOrderEntriesOf(reportData);
        assertThat(exportRate).isGreaterThan(50); // Should be reasonably fast
    }

    @Test
    @DisplayName("Interactive queries: Typed store supports efficient incremental updates")
    void typedStoreIncrementalUpdates() {
        String counterKey = "incremental_counter";
        int increments = 1000;

        Instant updateStart = Instant.now();

        // Simulate incremental counter updates (like event counters)
        for (int i = 0; i < increments; i++) {
            Long currentValue = typedRepository.getByKey(0, counterKey);
            Long newValue = (currentValue != null ? currentValue : 0L) + 1;
            typedRepository.save(0, counterKey, newValue);
        }

        Duration updateTime = Duration.between(updateStart, Instant.now());
        Long finalValue = typedRepository.getByKey(0, counterKey);

        LOG.info("Performed {} incremental updates in {}ms", increments, updateTime.toMillis());
        LOG.info("Final counter value: {}", finalValue);

        // Calculate performance metrics
        double updatesPerSecond = increments / (updateTime.toMillis() / 1000.0);
        LOG.info("Incremental update rate: {:.1f} updates/second", updatesPerSecond);

        // Verify correctness
        assertThat(finalValue).isEqualTo((long) increments);
        assertThat(updatesPerSecond).isGreaterThan(100); // Should be fast for incremental updates
    }

    @Test
    @DisplayName("Interactive queries: Typed store enables efficient filtered queries")
    void typedStoreFilteredQueries() {
        // Simulate user activity data with different categories
        Map<String, Long> userActivityData = Map.of(
                "user_active", 15420L,
                "user_inactive", 3420L,
                "user_new", 1250L,
                "user_returning", 8900L,
                "user_premium", 2100L,
                "user_basic", 11230L,
                "user_trial", 675L,
                "user_expired", 340L
        );

        userActivityData.forEach((activity, count) -> typedRepository.save(0, activity, count));

        Instant filterStart = Instant.now();

        // Simulate filtered query: get only active users
        Map<String, Long> activeUsers = new HashMap<>();
        userActivityData.entrySet().stream()
                .filter(entry -> entry.getKey().contains("active") || entry.getKey().contains("premium"))
                .forEach(entry -> {
                    Long value = typedRepository.getByKey(0, entry.getKey());
                    activeUsers.put(entry.getKey(), value);
                });

        Duration filterTime = Duration.between(filterStart, Instant.now());

        LOG.info("Filtered {} active user metrics in {}ms", activeUsers.size(), filterTime.toMillis());

        // Calculate active user statistics
        long totalActiveUsers = activeUsers.values().stream().mapToLong(Long::longValue).sum();
        LOG.info("Total active users: {}", totalActiveUsers);

        assertThat(activeUsers).hasSize(2); // active and premium
        assertThat(totalActiveUsers).isGreaterThan(0);
    }

    @Test
    @DisplayName("Interactive queries: Compare typed vs BLOB store performance")
    void compareTypedVsBlobPerformance() {
        // Setup both stores
        CassandraKeyValueStore blobStore = new CassandraKeyValueStore(
                "blob_performance_test",
                new PartitionedCassandraKeyValueStoreRepository(
                        session,
                        "blob_performance_test",
                        true,
                        "compaction = { 'class' : 'LeveledCompactionStrategy' }",
                        null,
                        null
                ),
                true
        );

        int testSize = 200;
        Map<String, Long> testData = new HashMap<>();

        // Generate test data
        IntStream.range(0, testSize).forEach(i -> {
            testData.put("perf_key_" + i, (long) i * 100);
        });

        // Test typed store performance
        Instant typedStart = Instant.now();
        testData.forEach((key, value) -> typedRepository.save(0, key, value));
        Duration typedInsertTime = Duration.between(typedStart, Instant.now());

        // Test BLOB store performance
        Instant blobStart = Instant.now();
        testData.forEach((key, value) -> {
            byte[] keyBytes = key.getBytes();
            byte[] valueBytes = value.toString().getBytes();
            blobStore.put(Bytes.wrap(keyBytes), valueBytes);
        });
        Duration blobInsertTime = Duration.between(blobStart, Instant.now());

        LOG.info("Typed store insert time: {}ms", typedInsertTime.toMillis());
        LOG.info("BLOB store insert time: {}ms", blobInsertTime.toMillis());
        LOG.info("Performance ratio: {:.2f}x",
                (double) blobInsertTime.toMillis() / typedInsertTime.toMillis());

        // Test retrieval performance
        Instant typedQueryStart = Instant.now();
        testData.keySet().forEach(key -> typedRepository.getByKey(0, key));
        Duration typedQueryTime = Duration.between(typedQueryStart, Instant.now());

        Instant blobQueryStart = Instant.now();
        testData.keySet().forEach(key -> {
            byte[] keyBytes = key.getBytes();
            blobStore.get(Bytes.wrap(keyBytes));
        });
        Duration blobQueryTime = Duration.between(blobQueryStart, Instant.now());

        LOG.info("Typed store query time: {}ms", typedQueryTime.toMillis());
        LOG.info("BLOB store query time: {}ms", blobQueryTime.toMillis());
        LOG.info("Query performance ratio: {:.2f}x",
                (double) blobQueryTime.toMillis() / typedQueryTime.toMillis());

        // Both should complete successfully
        assertThat(typedRepository.getCount()).isEqualTo(testSize);
    }
}
