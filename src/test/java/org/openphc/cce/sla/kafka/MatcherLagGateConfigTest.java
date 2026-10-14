package org.openphc.cce.sla.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the Kafka admin client settings built by {@link MatcherLagGateConfig} and the startup validation
 * in {@link MatcherLagGateProperties}.
 */
class MatcherLagGateConfigTest {

    /** Gate settings with the given thresholds, check timeout and extra client settings. */
    private static MatcherLagGateProperties buildGateProperties(long pauseAbove, long resumeAtOrBelow,
                                                                Duration checkTimeout,
                                                                Map<String, String>
                                                                        additionalKafkaAdminClientProperties) {
        return new MatcherLagGateProperties(true, false, "kafka:9092", "cce-matcher-service",
                "cce.events.inbound", pauseAbove, resumeAtOrBelow, checkTimeout, additionalKafkaAdminClientProperties);
    }

    /** The client's request and API timeouts are set to the check timeout. */
    @Test
    void kafkaAdminClientTimeoutsAreHeldToTheCheckBudget() {
        Map<String, Object> kafkaAdminClientConfig = MatcherLagGateConfig.buildKafkaAdminClientConfig(
                buildGateProperties(100, 5, Duration.ofSeconds(3), null));

        assertEquals("kafka:9092", kafkaAdminClientConfig.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(3000, kafkaAdminClientConfig.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
        assertEquals(3000, kafkaAdminClientConfig.get(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
    }

    /** Additional Kafka admin client properties reach the client and override the built-in defaults. */
    @Test
    void additionalKafkaAdminClientPropertiesPassThroughAndWinOverTheDefaults() {
        // How a secured cluster is reached without this service knowing about SASL or TLS.
        Map<String, String> additionalKafkaAdminClientProperties = Map.of("security.protocol", "SASL_SSL",
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "9000");
        Map<String, Object> kafkaAdminClientConfig = MatcherLagGateConfig.buildKafkaAdminClientConfig(
                buildGateProperties(100, 5, Duration.ofSeconds(5), additionalKafkaAdminClientProperties));

        assertEquals("SASL_SSL", kafkaAdminClientConfig.get("security.protocol"));
        assertEquals("9000", kafkaAdminClientConfig.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
    }

    /** Thresholds out of order, or a negative resume threshold, are rejected at startup. */
    @Test
    void resumeThresholdAbovePauseThreshold_refusesToStart() {
        // The gate could then never close for lag between the two, or flap at a single value.
        assertThrows(IllegalArgumentException.class,
                () -> buildGateProperties(10, 11, Duration.ofSeconds(5), null));
        assertThrows(IllegalArgumentException.class,
                () -> buildGateProperties(10, -1, Duration.ofSeconds(5), null));
    }

    /** A zero or negative check timeout is rejected at startup. */
    @Test
    void nonPositiveCheckTimeout_refusesToStart() {
        assertThrows(IllegalArgumentException.class,
                () -> buildGateProperties(100, 5, Duration.ZERO, null));
        assertThrows(IllegalArgumentException.class,
                () -> buildGateProperties(100, 5, Duration.ofSeconds(-1), null));
    }

    /** Equal thresholds are valid, and absent additional Kafka admin client properties become an empty map. */
    @Test
    void equalThresholdsAreAllowed() {
        assertDoesNotThrow(() -> buildGateProperties(5, 5, Duration.ofSeconds(5), null));
        assertTrue(buildGateProperties(5, 0, Duration.ofSeconds(5), null)
                .additionalKafkaAdminClientProperties().isEmpty());
    }
}
