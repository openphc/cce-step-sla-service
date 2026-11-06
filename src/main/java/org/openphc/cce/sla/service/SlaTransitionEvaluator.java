package org.openphc.cce.sla.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.openphc.cce.sla.kafka.MatcherLagGate;
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
 * <p>Every batch, the first included, is cleared by {@link MatcherLagGate} first. A cycle the gate holds
 * back does nothing at all; one it closes partway stops before its next batch, so a replay that starts
 * while a backlog is draining is not judged against for up to a hundred more batches.
 */
@Service
public class SlaTransitionEvaluator {

    private static final Logger log = LoggerFactory.getLogger(SlaTransitionEvaluator.class);

    /** Stops a pathological backlog or a fetch that never drains from monopolising a cycle. */
    private static final int MAX_BATCHES_PER_CYCLE = 100;

    private final SlaTransitionApplier applier;
    /** Asked before every batch whether the Matcher Service is caught up enough to evaluate. */
    private final MatcherLagGate matcherLagGate;
    private final int batchSize;
    /** Kept to register the skipped-cycle counter, which is tagged per reason and so built on first use. */
    private final MeterRegistry meterRegistry;
    private final Counter cycleCounter;
    private final Counter failedBatchCounter;

    public SlaTransitionEvaluator(SlaTransitionApplier applier,
                                  MatcherLagGate matcherLagGate,
                                  @Value("${cce.sla.batch-size:100}") int batchSize,
                                  MeterRegistry meterRegistry) {
        this.applier = applier;
        this.matcherLagGate = matcherLagGate;
        this.batchSize = batchSize;
        this.meterRegistry = meterRegistry;
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
            // One line on every scheduled cycle, running or paused, so the log shows each run happened and
            // what state the Matcher lag gate was in.
            // e.g. "SLA evaluation cycle: 0 transition(s) processed; running — Matcher Service lag is 3
            // message(s), pauses if it goes above 100"
            log.info("SLA evaluation cycle: {} transition(s) processed; {}",
                    fetchedThisCycle, matcherLagGate.describeCurrentGateState());
        } catch (RuntimeException e) {
            // Never let a cycle's failure kill the scheduler thread.
            log.error("SLA evaluation cycle failed", e);
        }
    }

    /**
     * Drain the due backlog, for as long as the Matcher lag gate allows.
     *
     * <p>The gate is asked before every batch, the first included. Before the first, it decides whether the
     * cycle runs at all; before each later one, it stops a cycle that a new backlog has made unsafe — a
     * replay that starts mid-drain creates already-due rows the next batch would otherwise fetch.
     *
     * @return how many rows were fetched across all batches; 0 for a cycle the gate held back
     */
    public int evaluateDue() {
        int fetchedThisCycle = 0;

        for (int batchIndex = 0; batchIndex < MAX_BATCHES_PER_CYCLE; batchIndex++) {
            // Ask the gate before touching the database: while the Matcher Service is behind, not one fetch.
            MatcherLagGate.GateDecision gateDecision = matcherLagGate.decideWhetherSlaEvaluationMayRun();
            if (!gateDecision.allowsSlaEvaluation()) {
                recordPausedCycle(gateDecision);
                return fetchedThisCycle;
            }
            // Counted once the first batch is allowed, so a cycle the gate held back is not counted as run.
            if (batchIndex == 0) {
                cycleCounter.increment();
            }
            List<UUID> fetchedIds = new ArrayList<>();
            int fetchedInBatch;
            try {
                fetchedInBatch = applier.fetchAndApply(fetchedIds);
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

            fetchedThisCycle += fetchedInBatch;
            if (fetchedInBatch < batchSize) {
                return fetchedThisCycle;
            }
        }

        log.warn("SLA evaluation stopped at the {}-batch cycle limit — backlog may still be draining",
                MAX_BATCHES_PER_CYCLE);
        return fetchedThisCycle;
    }

    /**
     * Counts a cycle the Matcher lag gate held back, whether before its first batch or partway through.
     *
     * <p>Not logged here: {@link #poll()} logs every cycle, paused ones included, with the gate's reason.
     *
     * @param gateDecision the gate's reason for holding evaluation back
     */
    private void recordPausedCycle(MatcherLagGate.GateDecision gateDecision) {
        // Tagged by reason, so the counter is built on first use of each tag rather than up front.
        Counter.builder("cce.sla.evaluator.cycles.skipped")
                .description("SLA evaluation cycles the Matcher lag gate held back, before or partway through")
                .tag("reason", gateDecision.skipReasonTag())
                .register(meterRegistry)
                .increment();
    }
}
