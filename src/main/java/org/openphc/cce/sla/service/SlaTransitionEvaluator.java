package org.openphc.cce.sla.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Drives SLA transition evaluation: polls for due rows and hands each batch to
 * {@link SlaTransitionApplier}.
 *
 * <p>Holds no transaction of its own. Batches are drained until one comes back short, so a backlog that
 * built up while the service was down is cleared in one cycle rather than one batch per interval, while a
 * steady state costs a single empty fetch query per interval.
 *
 * <p>One loop over one table settles every verdict, {@code MET} included — it arrives as a
 * {@code MET_CONDITION_REACHED} row like any other schedule, so there is no second pass with its own
 * failure mode to reason about.
 *
 * <p>Safe to run on every instance concurrently — the applier's {@code FOR UPDATE SKIP LOCKED} fetch is
 * what keeps them off each other's rows.
 *
 * <p>Once a batch has committed, the deviations it recorded go to {@link DeviationIntelligenceEvaluator}
 * for their intelligence actions. A deviation whose evaluation fails is contained there; it does not stop
 * the drain, since the SLA verdicts it came from are already committed.
 */
@Service
public class SlaTransitionEvaluator {

    private static final Logger log = LoggerFactory.getLogger(SlaTransitionEvaluator.class);

    /** Stops a pathological backlog or a fetch that never drains from monopolising a cycle. */
    private static final int MAX_BATCHES_PER_CYCLE = 100;

    private final SlaTransitionApplier applier;
    private final DeviationIntelligenceEvaluator deviationIntelligenceEvaluator;
    private final int batchSize;
    private final Counter cycleCounter;
    private final Counter failedBatchCounter;

    public SlaTransitionEvaluator(SlaTransitionApplier applier,
                                  DeviationIntelligenceEvaluator deviationIntelligenceEvaluator,
                                  @Value("${cce.sla.batch-size:100}") int batchSize,
                                  MeterRegistry meterRegistry) {
        this.applier = applier;
        this.deviationIntelligenceEvaluator = deviationIntelligenceEvaluator;
        this.batchSize = batchSize;
        this.cycleCounter = Counter.builder("cce.sla.evaluator.cycles")
                .description("SLA evaluation cycles run")
                .register(meterRegistry);
        this.failedBatchCounter = Counter.builder("cce.sla.evaluator.batches.failed")
                .description("SLA evaluation batches that rolled back and were backed off")
                .register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${cce.sla.poll-interval-ms:5000}")
    public void poll() {
        try {
            int fetchedThisCycle = evaluateDue();
            if (fetchedThisCycle > 0) {
                log.info("SLA evaluation cycle processed {} transition(s)", fetchedThisCycle);
            }
        } catch (RuntimeException e) {
            // Never let a cycle's failure kill the scheduler thread.
            log.error("SLA evaluation cycle failed", e);
        }
    }

    /**
     * Drain the due backlog.
     *
     * @return how many rows were fetched across all batches
     */
    public int evaluateDue() {
        cycleCounter.increment();
        int fetchedThisCycle = 0;

        for (int batchIndex = 0; batchIndex < MAX_BATCHES_PER_CYCLE; batchIndex++) {
            List<UUID> fetchedIds = new ArrayList<>();
            List<UUID> recordedDeviationIds = new ArrayList<>();
            int fetchedInBatch;
            try {
                fetchedInBatch = applier.fetchAndApply(fetchedIds, recordedDeviationIds);
            } catch (RuntimeException e) {
                // The batch rolled back, so nothing was marked processed and no deviation was written.
                // Back the fetched rows off in a fresh transaction so they are retried later rather than
                // on every cycle, then stop: whatever broke is likely to break the next batch too.
                failedBatchCounter.increment();
                log.error("SLA batch of {} row(s) failed and was rolled back — backing off",
                        fetchedIds.size(), e);
                if (!fetchedIds.isEmpty()) {
                    applier.backOff(fetchedIds);
                }
                return fetchedThisCycle;
            }

            deviationIntelligenceEvaluator.evaluate(recordedDeviationIds);
            fetchedThisCycle += fetchedInBatch;
            if (fetchedInBatch < batchSize) {
                return fetchedThisCycle;
            }
        }

        log.warn("SLA evaluation stopped at the {}-batch cycle limit — backlog may still be draining",
                MAX_BATCHES_PER_CYCLE);
        return fetchedThisCycle;
    }
}
