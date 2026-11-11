package org.openphc.cce.sla.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SlaTransitionEvaluatorTest {

    @Mock private SlaTransitionApplier applier;
    @Mock private DeviationIntelligenceEvaluator deviationIntelligenceEvaluator;

    private SlaTransitionEvaluator evaluator(int batchSize) {
        return new SlaTransitionEvaluator(applier, deviationIntelligenceEvaluator, batchSize, new SimpleMeterRegistry());
    }

    @Test
    void drainsUntilABatchComesBackShort() {
        // A backlog is cleared within one cycle rather than one batch per poll interval.
        when(applier.fetchAndApply(any(), any())).thenReturn(10, 10, 3);

        assertEquals(23, evaluator(10).evaluateDue());
        verify(applier, times(3)).fetchAndApply(any(), any());
    }

    @Test
    void emptyBacklogCostsASingleFetch() {
        when(applier.fetchAndApply(any(), any())).thenReturn(0);

        assertEquals(0, evaluator(10).evaluateDue());
        verify(applier, times(1)).fetchAndApply(any(), any());
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
        }).when(applier).fetchAndApply(any(), any());

        assertEquals(0, evaluator(10).evaluateDue());
        verify(applier).backOff(List.of(firstTransitionId, secondTransitionId));
    }

    @Test
    void failedBatch_stopsTheCycleRatherThanRetryingImmediately() {
        doThrow(new IllegalStateException("boom")).when(applier).fetchAndApply(any(), any());

        evaluator(10).evaluateDue();

        verify(applier, times(1)).fetchAndApply(any(), any());
    }

    @Test
    void committedBatch_handsTheDeviationsItRecordedToIntelligence() {
        UUID firstDeviationId = UUID.randomUUID();
        UUID secondDeviationId = UUID.randomUUID();
        recordDeviations(firstDeviationId, secondDeviationId);

        assertEquals(1, evaluator(10).evaluateDue());
        verify(deviationIntelligenceEvaluator).evaluate(List.of(firstDeviationId, secondDeviationId));
    }

    @Test
    void failedBatch_evaluatesNoIntelligence() {
        // A rolled-back batch recorded no deviation, whatever ids it had collected before failing.
        doAnswer(invocation -> {
            List<UUID> recordedDeviationIds = invocation.getArgument(1);
            recordedDeviationIds.add(UUID.randomUUID());
            throw new IllegalStateException("boom");
        }).when(applier).fetchAndApply(any(), any());

        evaluator(10).evaluateDue();

        verifyNoInteractions(deviationIntelligenceEvaluator);
    }

    @Test
    void poll_logsAndReturnsWhenWorkWasApplied() {
        when(applier.fetchAndApply(any(), any())).thenReturn(1);

        assertDoesNotThrow(() -> evaluator(10).poll());
        verify(applier).fetchAndApply(any(), any());
    }

    @Test
    void stopsAtTheCycleLimitRatherThanMonopolisingTheThread() {
        // A fetch that never drains must not spin forever inside one cycle.
        when(applier.fetchAndApply(any(), any())).thenReturn(5);

        assertEquals(500, evaluator(5).evaluateDue());
        verify(applier, times(100)).fetchAndApply(any(), any());
    }

    @Test
    void pollNeverPropagates_soTheSchedulerThreadSurvives() {
        doThrow(new IllegalStateException("boom")).when(applier).fetchAndApply(any(), any());

        assertDoesNotThrow(() -> evaluator(10).poll());
    }

    /** One short batch of a single row that recorded the given deviations. */
    private void recordDeviations(UUID... deviationIds) {
        doAnswer(invocation -> {
            List<UUID> recordedDeviationIds = invocation.getArgument(1);
            recordedDeviationIds.addAll(List.of(deviationIds));
            return 1;
        }).when(applier).fetchAndApply(any(), any());
    }
}
