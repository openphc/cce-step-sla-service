package org.openphc.cce.sla.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.common.entity.StepInstance;
import org.openphc.cce.common.entity.StepSlaStateTransition;
import org.openphc.cce.common.enums.DeviationType;
import org.openphc.cce.common.enums.SlaStatus;
import org.openphc.cce.common.enums.SlaTransitionType;
import org.openphc.cce.common.enums.StepStatus;
import org.openphc.cce.common.repository.StepInstanceRepository;
import org.openphc.cce.common.deviation.DeviationRecorder;
import org.openphc.cce.common.deviation.DeviationRecorder.PendingDeviation;
import org.openphc.cce.common.history.StateTransitionHistoryWriter;
import org.openphc.cce.sla.domain.repository.SlaTransitionFetchRepository;
import org.springframework.data.domain.Limit;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * The applier is the only writer of {@code step_instance.sla_status}, so these tests are the whole
 * specification of how a step's timeliness gets decided. Matcher records {@code completed_at}; every
 * judgement made from it is here.
 */
@ExtendWith(MockitoExtension.class)
class SlaTransitionApplierTest {

    @Mock private SlaTransitionFetchRepository transitionRepository;
    @Mock private StepInstanceRepository stepInstanceRepository;
    @Mock private DeviationRecorder deviationRecorder;
    @Mock private StateTransitionHistoryWriter stateTransitionHistoryWriter;

    private SlaTransitionApplier applier;
    private final OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

    @BeforeEach
    void setUp() {
        applier = new SlaTransitionApplier(transitionRepository, stepInstanceRepository,
                deviationRecorder, stateTransitionHistoryWriter,
                "test-instance", 100, 3600, new SimpleMeterRegistry());
    }

    // ── the work never arrived ──

    @Nested
    class OutstandingStep {

        @Test
        void dueDateReached_becomesOverdueWithAnOverdueDeviation() {
            StepInstance step = step(StepStatus.NOT_STARTED, null, "must", null);
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusMinutes(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.OVERDUE, step.getSlaStatus());
            // crossing a deadline says nothing about whether the event arrived
            assertEquals(StepStatus.NOT_STARTED, step.getStepStatus());
            assertDeviationsRecorded(new PendingDeviation(step, DeviationType.OVERDUE));
            assertTrue(transition.isProcessed());
            assertEquals("test-instance", transition.getProcessedBy());
        }

        @Test
        void missedDateReached_mandatory_becomesMissedWithAMissedDeviation() {
            StepInstance step = step(StepStatus.NOT_STARTED, SlaStatus.OVERDUE, "must", null);
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MISSED_DATE_REACHED, now.minusMinutes(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.MISSED, step.getSlaStatus());
            assertDeviationsRecorded(new PendingDeviation(step, DeviationType.MISSED));
        }

        @Test
        void missedDateReached_optional_recordsNeitherStatusNorDeviation() {
            // Nothing was required of an optional step, so nothing was breached by its not happening.
            // Whatever status it already carries is left exactly as it is.
            StepInstance step = step(StepStatus.NOT_STARTED, SlaStatus.OVERDUE, "could", null);
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MISSED_DATE_REACHED, now.minusMinutes(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.OVERDUE, step.getSlaStatus());
            assertDeviationsRecorded();
            assertTrue(transition.isProcessed());
        }

        @Test
        void breach_absentRequiredBehaviour_isTreatedAsOptional() {
            // "must" is the only thing that makes a step mandatory. An unstated requiredBehavior
            // states no requirement, so such a step is not judged either — the same reading the
            // matcher takes when it decides what to schedule and what to pre-create.
            StepInstance step = step(StepStatus.NOT_STARTED, null, null, null);
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusMinutes(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertNull(step.getSlaStatus());
            assertDeviationsRecorded();
            assertTrue(transition.isProcessed());
        }

        @Test
        void everyStatusWriteIsRecordedInHistory() {
            // Without this the time-driven half of a step's timeline is missing from the CDC stream:
            // a step that went overdue and was never completed would show only its creation.
            StepInstance step = step(StepStatus.NOT_STARTED, null, "must", null);
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusMinutes(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            verify(stateTransitionHistoryWriter)
                    .recordStepInstanceTransition(eq(step), any(OffsetDateTime.class));
        }
    }

    // ── the work arrived; completed_at decides ──

    @Nested
    class CompletedStep {

        @Test
        void completedBeforeItsDueDate_recordsNoBreachAndLeavesMetToTheSweep() {
            // A kept threshold is not a verdict. The row had no breach to detect, so it is consumed
            // without a status; MET is settled from the step by fetchAndSettleOnTime.
            StepInstance step = step(StepStatus.COMPLETED, null, "must", now.minusHours(3));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusHours(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertNull(step.getSlaStatus());
            assertDeviationsRecorded();
            assertTrue(transition.isProcessed());
        }

        @Test
        void completedAfterItsDueDate_isOverdueWithADeviation() {
            StepInstance step = step(StepStatus.COMPLETED, null, "must", now.minusMinutes(5));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusHours(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.OVERDUE, step.getSlaStatus());
            assertDeviationsRecorded(new PendingDeviation(step, DeviationType.OVERDUE));
        }

        @Test
        void completedBetweenItsThresholds_staysOverdueRatherThanBecomingMet() {
            // The trap in the design: this step beat its missed date, but "did not breach this
            // threshold" only means MET at the due date. Reading it as MET here would relabel a late
            // completion as on time.
            StepInstance step = step(StepStatus.COMPLETED, SlaStatus.OVERDUE, "must", now.minusHours(2));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MISSED_DATE_REACHED, now.minusMinutes(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.OVERDUE, step.getSlaStatus());
            assertDeviationsRecorded();
            assertTrue(transition.isProcessed());
        }

        @Test
        void completedAfterItsMissedDate_isMissedWithADeviation() {
            StepInstance step = step(StepStatus.COMPLETED, SlaStatus.OVERDUE, "must", now.minusMinutes(5));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MISSED_DATE_REACHED, now.minusHours(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.MISSED, step.getSlaStatus());
            assertDeviationsRecorded(new PendingDeviation(step, DeviationType.MISSED));
        }

        @Test
        void completedAfterItsMissedDate_optional_recordsNoMissedDeviation() {
            // must-only, on this path as much as the outstanding one: otherwise optional work done
            // late would be penalised while the same work never done at all was not.
            StepInstance step = step(StepStatus.COMPLETED, SlaStatus.OVERDUE, "could", now.minusMinutes(5));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MISSED_DATE_REACHED, now.minusHours(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.OVERDUE, step.getSlaStatus());
            assertDeviationsRecorded();
        }

        @Test
        void completedAfterItsDueDate_optional_recordsNothing() {
            // An optional step is never late, because it was never required. Matcher writes no
            // schedule for one, so this row can only predate that rule — it is consumed, not judged.
            StepInstance step = step(StepStatus.COMPLETED, null, "could", now.minusMinutes(5));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusHours(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertNull(step.getSlaStatus());
            assertDeviationsRecorded();
            assertTrue(transition.isProcessed());
        }

        @Test
        void completedExactlyAtItsThreshold_isNotABreach() {
            // Thresholds are inclusive: work recorded at the due-date instant met it. A zero-offset
            // successor completed by its prerequisite's encounter lands exactly here, and its
            // MET_CONDITION_REACHED row carries the verdict.
            OffsetDateTime threshold = now.minusHours(1);
            StepInstance step = step(StepStatus.COMPLETED, null, "must", threshold);
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, threshold);
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertNull(step.getSlaStatus());
            assertDeviationsRecorded();
            assertTrue(transition.isProcessed());
        }

        @Test
        void completedOneMicrosecondAfterItsThreshold_countsAsABreach() {
            OffsetDateTime threshold = now.minusHours(1);
            StepInstance step = step(StepStatus.COMPLETED, null, "must", threshold.plusNanos(1_000));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, threshold);
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.OVERDUE, step.getSlaStatus());
            assertDeviationsRecorded(new PendingDeviation(step, DeviationType.OVERDUE));
        }

        @Test
        void completedWithNoTimestamp_isTreatedAsABreach() {
            // The row is better evidence than a missing timestamp, and letting it pass would hide
            // the gap rather than surface it.
            StepInstance step = step(StepStatus.COMPLETED, null, "must", null);
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusHours(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.OVERDUE, step.getSlaStatus());
            assertDeviationsRecorded(new PendingDeviation(step, DeviationType.OVERDUE));
        }
    }

    // ── the row's schedule, not the step's due date ──

    @Nested
    class WhichSideDecidesWhat {

        @Test
        void overdueIsMeasuredAgainstTheRowsProcessBy() {
            // A breach is the schedule's question. completed_at is past process_by, so the row records
            // OVERDUE — even though the step's own due date, later here, was beaten.
            StepInstance step = step(StepStatus.COMPLETED, null, "must", now.minusHours(3));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusHours(4));
            step.setDueDate(now.minusHours(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.OVERDUE, step.getSlaStatus());
            assertDeviationsRecorded(new PendingDeviation(step, DeviationType.OVERDUE));
        }

        @Test
        void theMissedDateRowIsStillJudgedByItsOwnSchedule() {
            // The missed date is not stored on the step, so that row's process_by is the threshold. A
            // due_date far in the past must not drag the missed-date verdict with it.
            StepInstance step = step(StepStatus.COMPLETED, null, "must", now.minusHours(3));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MISSED_DATE_REACHED, now.minusHours(1));
            step.setDueDate(now.minusDays(30));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            // Completed before the missed date: not written off, and no verdict of its own to give.
            assertDeviationsRecorded();
            assertTrue(transition.isProcessed());
        }
    }

    // ── the work arrived early; its own row carries the verdict ──

    @Nested
    class MetCondition {

        @Test
        void aStepRecordedBeforeItsDueDateIsSettledMet() {
            // Matcher writes this row when the completing event lands, with process_by = completed_at,
            // so the verdict is reached on the next cycle rather than at a due date weeks away.
            StepInstance step = step(StepStatus.COMPLETED, null, "must", now.minusHours(3));
            step.setDueDate(now.plusDays(4));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MET_CONDITION_REACHED, now.minusHours(3));
            fetch(transition);

            int fetchedCount = applier.fetchAndApply(new ArrayList<>());

            assertEquals(1, fetchedCount);
            assertEquals(SlaStatus.MET, step.getSlaStatus());
            // Nothing deviant about work done on time.
            assertDeviationsRecorded();
            assertTrue(transition.isProcessed());
        }

        @Test
        void aStepRecordedExactlyAtItsDueDateIsSettledMet() {
            OffsetDateTime due = now.minusHours(3);
            StepInstance step = step(StepStatus.COMPLETED, null, "must", due);
            step.setDueDate(due);
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MET_CONDITION_REACHED, due);
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.MET, step.getSlaStatus());
            assertDeviationsRecorded();
            assertTrue(transition.isProcessed());
        }

        @Test
        void theMetTransitionIsRecordedInHistory() {
            StepInstance step = step(StepStatus.COMPLETED, null, "must", now.minusHours(3));
            step.setDueDate(now.plusDays(4));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MET_CONDITION_REACHED, now.minusHours(3));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            verify(stateTransitionHistoryWriter).recordStepInstanceTransition(eq(step), any());
        }

        @Test
        void anAlreadyJudgedStepIsRefusedRatherThanRelabelled() {
            // Timeliness is settled once decided: a step some deadline already judged must not be told
            // retrospectively that it was on time.
            StepInstance step = step(StepStatus.COMPLETED, SlaStatus.OVERDUE, "must", now.minusHours(3));
            step.setDueDate(now.plusDays(4));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MET_CONDITION_REACHED, now.minusHours(3));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.OVERDUE, step.getSlaStatus());
            verify(stateTransitionHistoryWriter, never()).recordStepInstanceTransition(any(), any());
            assertTrue(transition.isProcessed());
        }

        @Test
        void aStepThatNoLongerReadsAsOnTimeRecordsNothing() {
            // The row schedules the question; the answer is still read off the step here. A row whose
            // step does not beat its due date settles nothing, whatever the row says.
            StepInstance step = step(StepStatus.COMPLETED, null, "must", now.minusHours(1));
            step.setDueDate(now.minusHours(3));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MET_CONDITION_REACHED, now.minusHours(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertNull(step.getSlaStatus());
            assertDeviationsRecorded();
            assertTrue(transition.isProcessed());
        }

        @Test
        void aMetRowForAnOptionalStepRecordsNothing() {
            // An optional step reaches no verdict at all — not MET either. Matcher writes no such row,
            // so this one predates the rule, and the applier enforces it rather than trusting the row.
            StepInstance step = step(StepStatus.COMPLETED, null, "could", now.minusHours(3));
            step.setDueDate(now.plusDays(4));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MET_CONDITION_REACHED, now.minusHours(3));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertNull(step.getSlaStatus());
            verify(stateTransitionHistoryWriter, never()).recordStepInstanceTransition(any(), any());
            assertTrue(transition.isProcessed());
        }

        @Test
        void aMetRowForAStepStillAwaitingItsEventRecordsNothing() {
            StepInstance step = step(StepStatus.NOT_STARTED, null, "must", null);
            step.setDueDate(now.plusDays(4));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.MET_CONDITION_REACHED, now.minusHours(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertNull(step.getSlaStatus());
            assertTrue(transition.isProcessed());
        }
    }

    @Nested
    class BatchCapacity {

        @Test
        void theOnlyFetchAsksForTheConfiguredBatchSize() {
            // One query, so the whole batch is its to fill — there is no room left over for a second.
            SlaTransitionApplier smallBatch = applierWithBatchSize(3);
            when(transitionRepository.fetchTransitions(any(), any())).thenReturn(List.of());

            smallBatch.fetchAndApply(new ArrayList<>());

            ArgumentCaptor<Limit> limit = ArgumentCaptor.forClass(Limit.class);
            verify(transitionRepository).fetchTransitions(any(), limit.capture());
            assertEquals(3, limit.getValue().max());
        }

        @Test
        void theConfiguredMaximumIsAccepted() {
            // The fetch locks every step in the batch for the whole transaction, so 100 is not an
            // arbitrary default — it is measured (see MAX_BATCH_SIZE's Javadoc). The boundary itself
            // must still start up cleanly.
            assertDoesNotThrow(() -> applierWithBatchSize(100));
        }

        @Test
        void aBatchSizePastTheMeasuredCeilingFailsAtStartup() {
            // Fail when the bean is constructed, not on the first oversized batch in production.
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> applierWithBatchSize(101));
            assertTrue(ex.getMessage().contains("101"));
            assertTrue(ex.getMessage().contains("100"));
        }

        private SlaTransitionApplier applierWithBatchSize(int batchSize) {
            return new SlaTransitionApplier(transitionRepository, stepInstanceRepository,
                    deviationRecorder, stateTransitionHistoryWriter,
                    "test-instance", batchSize, 3600, new SimpleMeterRegistry());
        }
    }

    // ── the forward-only rule ──

    @Nested
    class OutOfOrderApplication {

        @Test
        void overdueDoesNotOverwriteMissed() {
            // Rows are fetched oldest-deadline-first, but a retried batch can still land out of
            // order. Re-applying the due date must not walk a written-off step back to OVERDUE.
            StepInstance step = step(StepStatus.NOT_STARTED, SlaStatus.MISSED, "must", null);
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusHours(2));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.MISSED, step.getSlaStatus());
            verify(stepInstanceRepository, never()).save(any());
            assertTrue(transition.isProcessed());
        }

        @Test
        void metIsNotWrittenOverAnExistingJudgement() {
            // MET is written only from null. A step already found OVERDUE cannot be relabelled as
            // having been on time, however its rows are ordered.
            StepInstance step = step(StepStatus.COMPLETED, SlaStatus.OVERDUE, "must", now.minusDays(5));
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusHours(1));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(SlaStatus.OVERDUE, step.getSlaStatus());
            verify(stepInstanceRepository, never()).save(any());
        }

        @Test
        void reappliedBreachDoesNotRaiseASecondDeviation() {
            StepInstance step = step(StepStatus.NOT_STARTED, SlaStatus.OVERDUE, "must", null);
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusHours(2));
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertDeviationsRecorded();
        }
    }

    @Nested
    class Retry {

        @Test
        void backOff_defersRowsExponentiallyInTheAttemptCount() {
            StepSlaStateTransition transition = transitionFor(step(StepStatus.NOT_STARTED, null, "must", null),
                    SlaTransitionType.DUE_DATE_REACHED, now.minusMinutes(1));
            transition.setAttempts(3);
            OffsetDateTime before = transition.getNextAttemptAt();
            when(transitionRepository.findAllById(List.of(transition.getId()))).thenReturn(List.of(transition));

            applier.backOff(List.of(transition.getId()));

            assertTrue(transition.getNextAttemptAt().isAfter(before));
            assertEquals(4, transition.getAttempts());
        }

        @Test
        void backOff_isCappedSoABrokenRowIsStillRetriedOccasionally() {
            StepSlaStateTransition transition = transitionFor(step(StepStatus.NOT_STARTED, null, "must", null),
                    SlaTransitionType.DUE_DATE_REACHED, now.minusMinutes(1));
            transition.setAttempts(40);
            when(transitionRepository.findAllById(List.of(transition.getId()))).thenReturn(List.of(transition));

            applier.backOff(List.of(transition.getId()));

            assertFalse(transition.getNextAttemptAt().isAfter(OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(3601)));
        }

        @Test
        void backOff_leavesAnAlreadyProcessedRowAlone() {
            StepSlaStateTransition transition = transitionFor(step(StepStatus.NOT_STARTED, null, "must", null),
                    SlaTransitionType.DUE_DATE_REACHED, now.minusMinutes(1));
            transition.setProcessed(true);
            OffsetDateTime before = transition.getNextAttemptAt();
            when(transitionRepository.findAllById(List.of(transition.getId()))).thenReturn(List.of(transition));

            applier.backOff(List.of(transition.getId()));

            assertEquals(before, transition.getNextAttemptAt());
            verify(transitionRepository, never()).save(transition);
        }
    }

    @Nested
    class Bookkeeping {

        @Test
        void aBatchTakesItsStepsFromTheFetch_andAStepsRowsJudgeTheSameInstance() {
            // A step's rows can come round in the same batch. The fetch brings each row's step with
            // it, so nothing is loaded per row, and every row sees the same instance: the missed-date
            // row judges the step the due-date row has just written OVERDUE.
            StepInstance step = step(StepStatus.NOT_STARTED, null, "must", null);
            StepSlaStateTransition dueTransition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusHours(2));
            StepSlaStateTransition missedTransition = transitionFor(step, SlaTransitionType.MISSED_DATE_REACHED, now.minusMinutes(1));
            when(transitionRepository.fetchTransitions(any(), any())).thenReturn(List.of(dueTransition, missedTransition));

            int fetchedCount = applier.fetchAndApply(new ArrayList<>());

            assertEquals(2, fetchedCount);
            verify(stepInstanceRepository, never()).findAllById(anyIterable());
            verify(stepInstanceRepository, never()).findById(any());
            assertEquals(SlaStatus.MISSED, step.getSlaStatus());
            // Both of the batch's deviations go to the recorder in one call, in the order they were found.
            assertDeviationsRecorded(
                    new PendingDeviation(step, DeviationType.OVERDUE),
                    new PendingDeviation(step, DeviationType.MISSED));
            assertTrue(dueTransition.isProcessed());
            assertTrue(missedTransition.isProcessed());
        }

        @Test
        void allThreeOfAStepsRowsAreJudgedFromOneFetch() {
            // The most a step can hold: both thresholds and its MET condition. Ordered by process_by,
            // the MET row comes first — written at the completion, which is what beat the due date —
            // and the two deadlines that follow find the step settled and record nothing.
            StepInstance step = step(StepStatus.COMPLETED, null, "must", now.minusDays(2));
            StepSlaStateTransition met = transitionFor(step, SlaTransitionType.MET_CONDITION_REACHED, now.minusDays(2));
            StepSlaStateTransition due = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusHours(2));
            StepSlaStateTransition missed = transitionFor(step, SlaTransitionType.MISSED_DATE_REACHED, now.minusHours(1));
            when(transitionRepository.fetchTransitions(any(), any())).thenReturn(List.of(met, due, missed));

            int fetchedCount = applier.fetchAndApply(new ArrayList<>());

            assertEquals(3, fetchedCount);
            verify(stepInstanceRepository, never()).findAllById(anyIterable());
            assertEquals(SlaStatus.MET, step.getSlaStatus());
            assertDeviationsRecorded();
            // One write, not three: the deadlines had nothing left to say about a settled step.
            verify(stateTransitionHistoryWriter, times(1)).recordStepInstanceTransition(eq(step), any());
            assertTrue(met.isProcessed());
            assertTrue(due.isProcessed());
            assertTrue(missed.isProcessed());
        }

        @Test
        void fetchReportsEveryRowItTook_soAFailedBatchCanBeBackedOff() {
            StepInstance step = step(StepStatus.NOT_STARTED, null, "must", null);
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusMinutes(1));
            fetch(transition);
            List<UUID> fetchedIds = new ArrayList<>();

            int fetchedCount = applier.fetchAndApply(fetchedIds);

            assertEquals(1, fetchedCount);
            assertEquals(List.of(transition.getId()), fetchedIds);
        }

        @Test
        void repeatedFailuresAreEscalatedOnFetch() {
            StepInstance step = step(StepStatus.NOT_STARTED, null, "must", null);
            StepSlaStateTransition transition = transitionFor(step, SlaTransitionType.DUE_DATE_REACHED, now.minusMinutes(1));
            transition.setAttempts(9);
            fetch(transition);

            applier.fetchAndApply(new ArrayList<>());

            assertEquals(10, transition.getAttempts());
        }
    }

    private void fetch(StepSlaStateTransition transition) {
        when(transitionRepository.fetchTransitions(any(), any())).thenReturn(List.of(transition));
    }

    /** The batch's deviations, handed to the recorder in one call — an empty call when there were none. */
    private void assertDeviationsRecorded(PendingDeviation... expected) {
        verify(deviationRecorder).recordDeviations(List.of(expected));
        verify(deviationRecorder, never()).recordDeviation(any(), any());
    }

    private StepInstance step(StepStatus stepStatus, SlaStatus slaStatus,
                             String requiredBehavior, OffsetDateTime completedAt) {
        return StepInstance.builder()
                .id(UUID.randomUUID())
                .actionId("anc-visit-1")
                .repeatIndex(0)
                .stepStatus(stepStatus)
                .slaStatus(slaStatus)
                .requiredBehavior(requiredBehavior)
                .completedAt(completedAt)
                .build();
    }

    private StepSlaStateTransition transitionFor(StepInstance step, SlaTransitionType type,
                                       OffsetDateTime processBy) {
        if (type == SlaTransitionType.DUE_DATE_REACHED) {
            // What the Matcher does: the deadline is written on the step and scheduled on the row from
            // the same value, in one transaction. Tests that need them to differ set the step's own.
            step.setDueDate(processBy);
        }
        return StepSlaStateTransition.builder()
                .id(UUID.randomUUID())
                .stepInstance(step)
                .transitionType(type)
                .processBy(processBy)
                .nextAttemptAt(processBy)
                .build();
    }
}
