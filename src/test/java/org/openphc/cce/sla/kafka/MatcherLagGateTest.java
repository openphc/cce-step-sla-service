package org.openphc.cce.sla.kafka;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.openphc.cce.sla.kafka.MatcherLagGate.GateDecision;
import org.openphc.cce.sla.kafka.MatcherLagReader.MatcherLag;
import org.openphc.cce.sla.kafka.MatcherLagReader.MatcherLagUnavailableException;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link MatcherLagGate}: the pause and resume thresholds, fail-closed behaviour, force-pause,
 * the state description and the metrics. The reader is a mock, so each test sets
 * the lag and the time directly.
 */
class MatcherLagGateTest {

    private final MatcherLagReader lagReader = mock(MatcherLagReader.class);
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    /** Gate settings with the production thresholds (pause above 100, resume at or below 5). */
    private static MatcherLagGateProperties buildGateProperties(boolean enabled, boolean forcePause) {
        return new MatcherLagGateProperties(enabled, forcePause, "localhost:9092", "cce-matcher-service",
                "cce.events.inbound", 100, 5, Duration.ofSeconds(5), null);
    }

    /** An enabled gate with production thresholds and no force-pause. */
    private MatcherLagGate newEnabledGate() {
        return newGateWith(buildGateProperties(true, false));
    }

    /** Builds a gate over the mocked reader. */
    private MatcherLagGate newGateWith(MatcherLagGateProperties gateProperties) {
        return new MatcherLagGate(gateProperties, lagReader, meterRegistry);
    }

    /** Makes the next reads return this total lag, all on partition 0. */
    private void stubMatcherLag(long totalLag) {
        when(lagReader.readLagPerPartition()).thenReturn(new MatcherLag(Map.of(0, totalLag)));
    }

    /** Makes the next reads fail, as they would with Kafka unreachable. */
    private void stubMatcherLagUnreadable() {
        when(lagReader.readLagPerPartition()).thenThrow(new MatcherLagUnavailableException("down", null));
    }

    /** A disabled gate lets every cycle run and never calls Kafka. */
    @Test
    void disabled_alwaysProcessesAndNeverReadsTheLag() {
        MatcherLagGate disabledGate = new MatcherLagGate(buildGateProperties(false, false), null, meterRegistry);

        assertEquals(GateDecision.PROCESS, disabledGate.decideWhetherSlaEvaluationMayRun());
        assertEquals(0.0, meterRegistry.get("cce.sla.gate.paused").gauge().value());
        verifyNoInteractions(lagReader);
    }

    /** A new gate starts paused, so a lag between the two thresholds keeps it paused. */
    @Test
    void startsClosed_soARestartMidDrainWaitsForTheLagToFall() {
        // 50 is below pause-above but not yet drained: resuming here would judge against the replay's tail.
        stubMatcherLag(50);

        assertEquals(GateDecision.PAUSED_LAG, newEnabledGate().decideWhetherSlaEvaluationMayRun());
    }

    /** A lag at the resume threshold opens a fresh lagGate. */
    @Test
    void opensOnceTheLagIsAtOrBelowResumeThreshold() {
        stubMatcherLag(5);

        assertEquals(GateDecision.PROCESS, newEnabledGate().decideWhetherSlaEvaluationMayRun());
    }

    /** An open gate stays open up to and including pause-above, and closes just past it. */
    @Test
    void openGate_toleratesLagUpToPauseAbove() {
        MatcherLagGate lagGate = newEnabledGate();
        stubMatcherLag(0);
        lagGate.decideWhetherSlaEvaluationMayRun();

        stubMatcherLag(100);
        assertEquals(GateDecision.PROCESS, lagGate.decideWhetherSlaEvaluationMayRun(),
                "pause-above is exclusive: 100 itself keeps running");

        stubMatcherLag(101);
        assertEquals(GateDecision.PAUSED_LAG, lagGate.decideWhetherSlaEvaluationMayRun());
    }

    /** A closed gate ignores lag falling below pause-above and reopens only at resume-at-or-below. */
    @Test
    void closedGate_staysClosedUntilTheLagDrainsRatherThanReopeningBelowPauseAbove() {
        MatcherLagGate lagGate = newEnabledGate();
        stubMatcherLag(0);
        lagGate.decideWhetherSlaEvaluationMayRun();
        stubMatcherLag(15_000);
        lagGate.decideWhetherSlaEvaluationMayRun();

        stubMatcherLag(99);
        assertEquals(GateDecision.PAUSED_LAG, lagGate.decideWhetherSlaEvaluationMayRun());
        stubMatcherLag(6);
        assertEquals(GateDecision.PAUSED_LAG, lagGate.decideWhetherSlaEvaluationMayRun());
        stubMatcherLag(5);
        assertEquals(GateDecision.PROCESS, lagGate.decideWhetherSlaEvaluationMayRun());
    }

    /** An unreadable lag pauses the gate, and recovery needs the lag to drain, not just to be readable. */
    @Test
    void unreadableLag_pausesAndMustThenDrainLikeAnyOtherPause() {
        // Fail-closed: if Kafka cannot be asked, the Matcher Service is most likely not consuming either.
        MatcherLagGate lagGate = newEnabledGate();
        stubMatcherLag(0);
        assertEquals(GateDecision.PROCESS, lagGate.decideWhetherSlaEvaluationMayRun());

        stubMatcherLagUnreadable();
        assertEquals(GateDecision.PAUSED_UNAVAILABLE, lagGate.decideWhetherSlaEvaluationMayRun());
        assertEquals(GateDecision.PAUSED_UNAVAILABLE, lagGate.decideWhetherSlaEvaluationMayRun());

        // Back, but with a backlog built while it was unreadable: 50 is not low enough to resume.
        reset(lagReader);
        stubMatcherLag(50);
        assertEquals(GateDecision.PAUSED_LAG, lagGate.decideWhetherSlaEvaluationMayRun());
        stubMatcherLag(0);
        assertEquals(GateDecision.PROCESS, lagGate.decideWhetherSlaEvaluationMayRun());
    }

    /** force-pause holds the gate at zero lag, while the lag gauge keeps reporting. */
    @Test
    void forcePause_holdsEvenWithNoLag_butStillReadsItForTheMetrics() {
        // For a slow replay that never builds lag. The operator watches the lag gauge to know when to lift it.
        MatcherLagGate forcePausedGate = newGateWith(buildGateProperties(true, true));
        stubMatcherLag(0);

        assertEquals(GateDecision.PAUSED_FORCED, forcePausedGate.decideWhetherSlaEvaluationMayRun());
        assertEquals(0.0, meterRegistry.get("cce.sla.matcher.lag").gauge().value());
        assertEquals(1.0, meterRegistry.get("cce.sla.gate.paused").gauge().value());
    }

    /** force-pause takes precedence over an unreadable lag. */
    @Test
    void forcePause_holdsWhenTheLagCannotBeReadToo() {
        MatcherLagGate forcePausedGate = newGateWith(buildGateProperties(true, true));
        stubMatcherLagUnreadable();

        assertEquals(GateDecision.PAUSED_FORCED, forcePausedGate.decideWhetherSlaEvaluationMayRun());
        assertTrue(Double.isNaN(meterRegistry.get("cce.sla.matcher.lag").gauge().value()));
    }


    /** A successful read updates the total gauge, the per-partition gauges and the paused gauge. */
    @Test
    void publishesTotalAndPerPartitionLag() {
        MatcherLagGate lagGate = newEnabledGate();
        when(lagReader.readLagPerPartition()).thenReturn(new MatcherLag(Map.of(0, 1L, 1, 0L, 2, 4L)));

        lagGate.decideWhetherSlaEvaluationMayRun();

        assertEquals(5.0, meterRegistry.get("cce.sla.matcher.lag").gauge().value());
        assertEquals(4.0, meterRegistry.get("cce.sla.matcher.lag.partition").tag("partition", "2").gauge().value());
        assertEquals(0.0, meterRegistry.get("cce.sla.gate.paused").gauge().value());
    }

    /** A failed read sets the total to NaN and removes the per-partition rows. */
    @Test
    void unreadableLag_clearsTheLagMetricsRatherThanLeavingAStaleValue() {
        MatcherLagGate lagGate = newEnabledGate();
        stubMatcherLag(3);
        lagGate.decideWhetherSlaEvaluationMayRun();

        stubMatcherLagUnreadable();
        lagGate.decideWhetherSlaEvaluationMayRun();

        assertTrue(Double.isNaN(meterRegistry.get("cce.sla.matcher.lag").gauge().value()));
        assertTrue(meterRegistry.find("cce.sla.matcher.lag.partition").gauges().isEmpty());
        assertEquals(1.0, meterRegistry.get("cce.sla.gate.paused").gauge().value());
    }

    /** Before any check, an enabled gate reports itself paused, matching its starting state. */
    @Test
    void pausedGaugeReadsPausedBeforeTheFirstCheck() {
        newEnabledGate();

        assertEquals(1.0, meterRegistry.get("cce.sla.gate.paused").gauge().value());
    }

    /** Only PROCESS allows processing, and each decision maps to its metric tag value. */
    @Test
    void skipReasonTagsAreTheMetricTagValues() {
        assertTrue(GateDecision.PROCESS.allowsSlaEvaluation());
        assertFalse(GateDecision.PAUSED_LAG.allowsSlaEvaluation());
        assertEquals("none", GateDecision.PROCESS.skipReasonTag());
        assertEquals("lag", GateDecision.PAUSED_LAG.skipReasonTag());
        assertEquals("unavailable", GateDecision.PAUSED_UNAVAILABLE.skipReasonTag());
        assertEquals("forced", GateDecision.PAUSED_FORCED.skipReasonTag());
    }

    /** Before any check the state is "not checked yet"; while running it gives the lag and the pause threshold. */
    @Test
    void describeCurrentGateState_whenNotCheckedYetOrRunning() {
        MatcherLagGate lagGate = newEnabledGate();
        assertEquals("not checked yet", lagGate.describeCurrentGateState());

        stubMatcherLag(3);
        lagGate.decideWhetherSlaEvaluationMayRun();
        assertEquals("running — Matcher Service lag is 3 message(s), pauses if it goes above 100",
                lagGate.describeCurrentGateState());
    }

    /** A disabled gate says so, since it never reads the lag. */
    @Test
    void describeCurrentGateState_whenDisabled() {
        MatcherLagGate disabledGate = new MatcherLagGate(buildGateProperties(false, false), null, meterRegistry);

        assertEquals("disabled — SLA evaluation runs regardless of Matcher Service lag",
                disabledGate.describeCurrentGateState());
    }

    /** A lag pause names the lag, the group and topic, and both thresholds. */
    @Test
    void describeCurrentGateState_forLag_givesTheLagAndThresholds() {
        MatcherLagGate lagGate = newEnabledGate();
        stubMatcherLag(512);
        lagGate.decideWhetherSlaEvaluationMayRun();

        assertEquals("paused — Matcher Service lag is 512 message(s) on group cce-matcher-service, topic "
                + "cce.events.inbound — paused once lag went above 100, resumes when it is at or below 5",
                lagGate.describeCurrentGateState());
    }

    /** An unreadable lag names the cluster, group and topic it could not read. */
    @Test
    void describeCurrentGateState_forUnreadableLag_namesWhereItLooked() {
        MatcherLagGate lagGate = newEnabledGate();
        stubMatcherLagUnreadable();
        lagGate.decideWhetherSlaEvaluationMayRun();

        assertEquals("paused — Matcher Service lag could not be read from Kafka (localhost:9092, "
                + "group cce-matcher-service, topic cce.events.inbound) — paused until it can be read and is "
                + "at or below 5",
                lagGate.describeCurrentGateState());
    }

    /** A forced pause says so, and still reports the lag, or "unknown" when it cannot be read. */
    @Test
    void describeCurrentGateState_forForcePause_saysItIsForcedAndGivesTheLag() {
        MatcherLagGate forcePausedGate = newGateWith(buildGateProperties(true, true));
        stubMatcherLag(3);
        forcePausedGate.decideWhetherSlaEvaluationMayRun();
        assertEquals("paused — cce.sla.matcher-lag-gate.force-pause is set (Matcher Service lag is 3 "
                + "message(s)) — paused until it is unset",
                forcePausedGate.describeCurrentGateState());

        stubMatcherLagUnreadable();
        forcePausedGate.decideWhetherSlaEvaluationMayRun();
        assertTrue(forcePausedGate.describeCurrentGateState().contains("(Matcher Service lag is unknown)"));
    }
}
