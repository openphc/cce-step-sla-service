package org.openphc.cce.sla.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.common.entity.Deviation;
import org.openphc.cce.common.entity.IntelligenceEventLog;
import org.openphc.cce.common.entity.StepInstance;
import org.openphc.cce.common.enums.DeviationType;
import org.openphc.cce.common.intelligence.IntelligenceActionEvaluator;
import org.openphc.cce.common.repository.DeviationRepository;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeviationIntelligenceEvaluatorTest {

    @Mock private DeviationRepository deviationRepository;
    @Mock private IntelligenceActionEvaluator intelligenceActionEvaluator;
    @Mock private PlatformTransactionManager transactionManager;

    private SimpleMeterRegistry meterRegistry;
    private DeviationIntelligenceEvaluator evaluator;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        evaluator = new DeviationIntelligenceEvaluator(
                deviationRepository, intelligenceActionEvaluator, transactionManager, meterRegistry);
    }

    @Test
    void evaluatesEachDeviationAgainstItsOwnStep() {
        Deviation first = recordedDeviation(2);
        Deviation second = recordedDeviation(1);

        assertEquals(3, evaluator.evaluate(List.of(first.getId(), second.getId())));
        verify(intelligenceActionEvaluator).evaluateOnDeviation(first.getStepInstance(), first);
        verify(intelligenceActionEvaluator).evaluateOnDeviation(second.getStepInstance(), second);
    }

    @Test
    void eachDeviationRunsInATransactionOfItsOwn() {
        Deviation first = recordedDeviation(1);
        Deviation second = recordedDeviation(1);

        evaluator.evaluate(List.of(first.getId(), second.getId()));

        verify(transactionManager, times(2)).getTransaction(any());
        verify(transactionManager, times(2)).commit(any());
    }

    @Test
    void aFailedDeviation_rollsBackAlone_andTheOthersCarryOn() {
        // The verdicts are committed; one deviation's alert failing must not cost the others theirs.
        Deviation failing = recordedDeviation(0);
        Deviation next = recordedDeviation(1);
        when(intelligenceActionEvaluator.evaluateOnDeviation(failing.getStepInstance(), failing))
                .thenThrow(new IllegalStateException("boom"));

        assertEquals(1, evaluator.evaluate(List.of(failing.getId(), next.getId())));
        verify(transactionManager).rollback(any());
        verify(transactionManager).commit(any());
        assertEquals(1.0, meterRegistry.counter("cce.sla.intelligence.failed").count());
    }

    @Test
    void aDeviationThatNeverCommitted_firesNothing() {
        UUID deviationId = UUID.randomUUID();
        when(deviationRepository.findById(deviationId)).thenReturn(Optional.empty());

        assertEquals(0, evaluator.evaluate(List.of(deviationId)));
        verify(intelligenceActionEvaluator, never()).evaluateOnDeviation(any(), any());
        assertEquals(0.0, meterRegistry.counter("cce.sla.intelligence.failed").count());
    }

    @Test
    void noDeviations_opensNoTransaction() {
        assertEquals(0, evaluator.evaluate(List.of()));
        verifyNoInteractions(transactionManager);
    }

    /** A committed overdue deviation whose evaluation fires {@code fired} intelligence actions. */
    private Deviation recordedDeviation(int fired) {
        StepInstance step = StepInstance.builder().id(UUID.randomUUID()).actionId("follow-up-visit").build();
        Deviation deviation = Deviation.builder()
                .id(UUID.randomUUID()).stepInstance(step).deviationType(DeviationType.OVERDUE).build();
        when(deviationRepository.findById(deviation.getId())).thenReturn(Optional.of(deviation));
        lenient().when(intelligenceActionEvaluator.evaluateOnDeviation(step, deviation))
                .thenReturn(Collections.nCopies(fired, new IntelligenceEventLog()));
        return deviation;
    }
}
