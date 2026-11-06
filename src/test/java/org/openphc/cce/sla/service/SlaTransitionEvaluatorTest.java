package org.openphc.cce.sla.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.sla.kafka.MatcherLagGate;
import org.openphc.cce.sla.kafka.MatcherLagGate.GateDecision;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SlaTransitionEvaluatorTest {

    @Mock private SlaTransitionApplier applier;
    /** The lag gate; open by default (see {@link #openGateByDefault()}), closed by the tests that need it. */
    @Mock private MatcherLagGate matcherLagGate;

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    /** Opens the gate for every test, so the tests that predate it run unchanged. */
    @BeforeEach
    void openGateByDefault() {
        lenient().when(matcherLagGate.decideWhetherSlaEvaluationMayRun()).thenReturn(GateDecision.PROCESS);
    }

    private SlaTransitionEvaluator evaluator(int batchSize) {
        return new SlaTransitionEvaluator(applier, matcherLagGate, batchSize, meterRegistry);
    }

    @Test
    void drainsUntilABatchComesBackShort() {
        // A backlog is cleared within one cycle rather than one batch per poll interval.
        when(applier.fetchAndApply(any())).thenReturn(10, 10, 3);

        assertEquals(23, evaluator(10).evaluateDue());
        verify(applier, times(3)).fetchAndApply(any());
    }

    @Test
    void emptyBacklogCostsASingleFetch() {
        when(applier.fetchAndApply(any())).thenReturn(0);

        assertEquals(0, evaluator(10).evaluateDue());
        verify(applier, times(1)).fetchAndApply(any());
    }

    @Test
    void failedBatch_backsOffTheRowsItHadFetched() {
        // The batch rolled back, so nothing was marked processed. The ids it collected before failing
        // are still in the caller's list, which is what lets them be deferred.
        UUID firstTransitionId = UUID.randomUUID();
        UUID secondTransitionId = UUID.randomUUID();
        doAnswer(invocation -> {
            List<UUID> fetchedIds = invocation.getArgument(0);
            fetchedIds.add(firstTransitionId);
            fetchedIds.add(secondTransitionId);
            throw new IllegalStateException("boom");
        }).when(applier).fetchAndApply(any());

        assertEquals(0, evaluator(10).evaluateDue());
        verify(applier).backOff(List.of(firstTransitionId, secondTransitionId));
    }

    @Test
    void failedBatch_stopsTheCycleRatherThanRetryingImmediately() {
        doThrow(new IllegalStateException("boom")).when(applier).fetchAndApply(any());

        evaluator(10).evaluateDue();

        verify(applier, times(1)).fetchAndApply(any());
    }

    @Test
    void poll_logsAndReturnsWhenWorkWasApplied() {
        when(applier.fetchAndApply(any())).thenReturn(1);

        assertDoesNotThrow(() -> evaluator(10).poll());
        verify(applier).fetchAndApply(any());
    }

    @Test
    void stopsAtTheCycleLimitRatherThanMonopolisingTheThread() {
        // A fetch that never drains must not spin forever inside one cycle.
        when(applier.fetchAndApply(any())).thenReturn(5);

        assertEquals(500, evaluator(5).evaluateDue());
        verify(applier, times(100)).fetchAndApply(any());
    }

    @Test
    void pollNeverPropagates_soTheSchedulerThreadSurvives() {
        doThrow(new IllegalStateException("boom")).when(applier).fetchAndApply(any());

        assertDoesNotThrow(() -> evaluator(10).poll());
    }

    /** A closed gate skips the cycle before any fetch, and counts it as skipped and not as run. */
    @Test
    void closedGate_skipsTheCycleWithoutTouchingTheDatabase() {
        // While the Matcher Service is behind, an absent completion proves nothing — not even one fetch.
        when(matcherLagGate.decideWhetherSlaEvaluationMayRun()).thenReturn(GateDecision.PAUSED_LAG);

        assertEquals(0, evaluator(10).evaluateDue());
        verifyNoInteractions(applier);
        assertEquals(1.0, meterRegistry.get("cce.sla.evaluator.cycles.skipped")
                .tag("reason", "lag").counter().count());
        assertEquals(0.0, meterRegistry.get("cce.sla.evaluator.cycles").counter().count());
    }

    /** Skipped cycles are counted separately for each reason. */
    @Test
    void skippedCycles_areCountedByReason() {
        when(matcherLagGate.decideWhetherSlaEvaluationMayRun())
                .thenReturn(GateDecision.PAUSED_UNAVAILABLE, GateDecision.PAUSED_FORCED,
                        GateDecision.PAUSED_UNAVAILABLE);
        SlaTransitionEvaluator evaluator = evaluator(10);

        evaluator.evaluateDue();
        evaluator.evaluateDue();
        evaluator.evaluateDue();

        assertEquals(2.0, meterRegistry.get("cce.sla.evaluator.cycles.skipped")
                .tag("reason", "unavailable").counter().count());
        assertEquals(1.0, meterRegistry.get("cce.sla.evaluator.cycles.skipped")
                .tag("reason", "forced").counter().count());
    }

    /** A gate that closes during a cycle stops it before the next batch, and counts the stop. */
    @Test
    void gateClosingPartway_stopsBeforeTheNextBatch() {
        // A replay that starts while a backlog drains must not be judged against for the rest of the cycle.
        when(matcherLagGate.decideWhetherSlaEvaluationMayRun())
                .thenReturn(GateDecision.PROCESS, GateDecision.PROCESS, GateDecision.PAUSED_LAG);
        when(applier.fetchAndApply(any())).thenReturn(10);

        assertEquals(20, evaluator(10).evaluateDue());
        verify(applier, times(2)).fetchAndApply(any());
        assertEquals(1.0, meterRegistry.get("cce.sla.evaluator.cycles.skipped")
                .tag("reason", "lag").counter().count());
        assertEquals(1.0, meterRegistry.get("cce.sla.evaluator.cycles").counter().count(),
                "a cycle stopped partway still ran, so it is counted as run too");
    }

    /** The gate is consulted once per batch. */
    @Test
    void gateIsAskedBeforeEveryBatch() {
        when(applier.fetchAndApply(any())).thenReturn(10, 10, 3);

        evaluator(10).evaluateDue();

        verify(matcherLagGate, times(3)).decideWhetherSlaEvaluationMayRun();
    }

    /** Every scheduled cycle logs the gate's state, whether the cycle ran or was held back. */
    @Test
    void poll_logsTheGateStateOnEveryCycle() {
        when(applier.fetchAndApply(any())).thenReturn(0);
        SlaTransitionEvaluator evaluator = evaluator(10);

        evaluator.poll();
        when(matcherLagGate.decideWhetherSlaEvaluationMayRun()).thenReturn(GateDecision.PAUSED_LAG);
        evaluator.poll();

        verify(matcherLagGate, times(2)).describeCurrentGateState();
    }
}
