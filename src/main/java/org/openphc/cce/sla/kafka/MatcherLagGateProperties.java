package org.openphc.cce.sla.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.Map;

/**
 * Settings for {@link MatcherLagGate}, bound from {@code cce.sla.matcher-lag-gate.*}.
 *
 * <p>Lag is the total across every partition of {@code topic}: the sum of each partition's
 * read-committed end offset minus the offset {@code groupId} has committed there. The gate closes when it
 * goes above {@code pauseAbove} and reopens only once it is at or below {@code resumeAtOrBelow}, so a
 * draining backlog is not judged against while its last events are still unmatched.
 *
 * @param enabled          off means every cycle runs and Kafka is never contacted
 * @param forcePause       holds every cycle whatever the lag — for a planned Event Replay
 * @param bootstrapServers the Kafka cluster the Matcher Service consumes from
 * @param groupId          the Matcher Service's consumer group
 * @param topic            the topic that group consumes
 * @param pauseAbove       total lag above which the gate closes
 * @param resumeAtOrBelow  total lag at or below which a closed gate reopens
 * @param checkTimeout     upper bound on one whole lag check; past it the check counts as failed
 * @param additionalKafkaAdminClientProperties
 *                         extra Kafka admin-client settings passed through as-is and applied last, so
 *                         they can add to or override the gate's own — e.g. {@code security.protocol} for a
 *                         secured cluster
 */
@ConfigurationProperties("cce.sla.matcher-lag-gate")
public record MatcherLagGateProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("false") boolean forcePause,
        @DefaultValue("localhost:9092") String bootstrapServers,
        @DefaultValue("cce-matcher-service") String groupId,
        @DefaultValue("cce.events.inbound") String topic,
        @DefaultValue("100") long pauseAbove,
        @DefaultValue("5") long resumeAtOrBelow,
        @DefaultValue("5s") Duration checkTimeout,
        Map<String, String> additionalKafkaAdminClientProperties) {

    /**
     * Validates the settings at startup, so a bad value stops the service from starting rather than
     * leaving a gate that can never open or close.
     *
     * @throws IllegalArgumentException if the thresholds are out of order or the check timeout is not positive
     */
    public MatcherLagGateProperties {
        // resume-at-or-below above pause-above would leave lag between the two with no defined state.
        if (resumeAtOrBelow < 0 || resumeAtOrBelow > pauseAbove) {
            throw new IllegalArgumentException("cce.sla.matcher-lag-gate.resume-at-or-below ("
                    + resumeAtOrBelow + ") must be between 0 and pause-above (" + pauseAbove + ")");
        }
        // A zero budget would fail every check, so the gate could never open.
        if (checkTimeout.isNegative() || checkTimeout.isZero()) {
            throw new IllegalArgumentException("cce.sla.matcher-lag-gate.check-timeout must be positive");
        }
        // Absent in application.yml binds as null; normalise to an empty, unmodifiable map.
        additionalKafkaAdminClientProperties = additionalKafkaAdminClientProperties == null
                ? Map.of()
                : Map.copyOf(additionalKafkaAdminClientProperties);
    }
}
