package org.openphc.cce.sla.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.openphc.cce.common.entity.Deviation;
import org.openphc.cce.common.intelligence.IntelligenceActionEvaluator;
import org.openphc.cce.common.repository.DeviationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Evaluates the intelligence actions of the deviations this service has recorded, and publishes those
 * that fire to {@code cce.intelligence.triggers}.
 *
 * <p>The PlanDefinition decides what a breach sets off: an intelligence action under the step whose
 * condition reads the deviation — {@code {"==": [{"var": "event.deviationType"}, "overdue"]}} — fires
 * when that step goes {@code OVERDUE}, and likewise for {@code missed}. The evaluation itself is
 * cce-common-util's {@link IntelligenceActionEvaluator#evaluateOnDeviation}, the same one the Matcher
 * Service runs for {@code ORDER_VIOLATION}: it writes the {@code intelligence_event_log} row, publishes
 * the trigger, and stamps the deviation's {@code intelligence_event_id}.
 *
 * <h2>After the batch, not inside it</h2>
 * {@link SlaTransitionEvaluator} calls this once {@link SlaTransitionApplier#fetchAndApply} has
 * committed. Publishing blocks until the broker acknowledges the record (up to
 * {@code cce.intelligence.publish-confirm-timeout-ms}); inside the batch that wait would be spent holding
 * every step of the batch under {@code FOR UPDATE}, so a slow broker would stall Matcher's writes to all
 * of them. Out here a transaction touches only the one deviation and its event log.
 *
 * <p>The cost is that the deviation and its alert no longer commit together. If the process dies between
 * the batch and this call, the deviation stays recorded with no {@code intelligence_event_id} and its
 * actions are never evaluated — the SLA verdict is safe, the alert is lost. A publish that fails or times
 * out is not that case: the evaluator commits the event log unpublished, which keeps it replayable.
 *
 * <h2>One transaction per deviation</h2>
 * Each deviation runs in a transaction of its own, opened through a {@link TransactionTemplate} rather
 * than {@code @Transactional}: the loop and the per-deviation work live in this one class, and a
 * {@code @Transactional} method called on its own class bypasses the proxy. A deviation whose evaluation
 * fails rolls back alone and is logged and counted; it does not stop the others.
 */
@Service
public class DeviationIntelligenceEvaluator {

    private static final Logger log = LoggerFactory.getLogger(DeviationIntelligenceEvaluator.class);

    private final DeviationRepository deviationRepository;
    private final IntelligenceActionEvaluator intelligenceActionEvaluator;
    private final TransactionTemplate transactionTemplate;
    private final Counter failedCounter;

    public DeviationIntelligenceEvaluator(DeviationRepository deviationRepository,
                                          IntelligenceActionEvaluator intelligenceActionEvaluator,
                                          PlatformTransactionManager transactionManager,
                                          MeterRegistry meterRegistry) {
        this.deviationRepository = deviationRepository;
        this.intelligenceActionEvaluator = intelligenceActionEvaluator;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.failedCounter = Counter.builder("cce.sla.intelligence.failed")
                .description("Recorded deviations whose intelligence actions could not be evaluated")
                .register(meterRegistry);
    }

    /**
     * Evaluate the intelligence actions of a committed batch's deviations, each in its own transaction.
     *
     * @return how many intelligence actions fired across them
     */
    public int evaluate(List<UUID> deviationIds) {
        int fired = 0;
        for (UUID deviationId : deviationIds) {
            try {
                fired += transactionTemplate.execute(status -> evaluateOne(deviationId));
            } catch (RuntimeException e) {
                // The deviation is committed; only its alert is lost. Retrying here would retry on every
                // cycle whatever broke it, so it is left for the logs and the counter.
                failedCounter.increment();
                log.error("Intelligence actions for deviation {} could not be evaluated", deviationId, e);
            }
        }
        return fired;
    }

    /**
     * The deviation is re-read rather than passed in: the batch that recorded it has committed and its
     * session is closed, and the evaluator walks the step to its protocol definition lazily.
     */
    private int evaluateOne(UUID deviationId) {
        Optional<Deviation> deviation = deviationRepository.findById(deviationId);
        if (deviation.isEmpty()) {
            // Only reachable if the batch's commit failed after the ids were collected.
            log.warn("Deviation {} not found — its intelligence actions are not evaluated", deviationId);
            return 0;
        }
        return intelligenceActionEvaluator
                .evaluateOnDeviation(deviation.get().getStepInstance(), deviation.get())
                .size();
    }
}
