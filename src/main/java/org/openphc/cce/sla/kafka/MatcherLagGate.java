package org.openphc.cce.sla.kafka;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import org.openphc.cce.sla.kafka.MatcherLagReader.MatcherLag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Decides whether an SLA cycle may run, from how far the Matcher Service is behind on its topic.
 *
 * <p>This is the in-process form of the Event Replay prerequisite (docs/architecture-overview.md): while
 * events sit unmatched in Kafka, an absent completion on {@code step_instance} does not mean the work was
 * not done, and a verdict reached then is permanent. Holding off costs only detection latency, because a
 * transition applied late reaches exactly the verdict it would have reached on time.
 *
 * <p>That asymmetry sets every rule here:
 * <ul>
 *   <li><b>Two thresholds.</b> The gate closes when total lag goes above {@code pause-above} and reopens
 *       only at or below {@code resume-at-or-below}. With one threshold, a draining replay would reopen
 *       the gate while its last events were still unmatched.</li>
 *   <li><b>Fail-closed.</b> A lag that cannot be read pauses the service and must then fall to
 *       {@code resume-at-or-below} like any other pause. If Kafka is unreachable the Matcher Service is not
 *       consuming either, so a backlog is the likelier truth.</li>
 *   <li><b>Starts closed.</b> A restart in the middle of a drain must not resume at a lag that is below
 *       {@code pause-above} but not yet drained.</li>
 *   <li><b>{@code force-pause}</b> holds every cycle whatever the lag — for a replay published slowly
 *       enough that the lag never builds. The lag is still read, so the metrics show when it has
 *       drained.</li>
 * </ul>
 *
 * <p>Every call reads the lag from Kafka afresh; nothing is cached. In steady state that is one read per
 * poll. During a drain it is one read per batch, so a backlog that appears mid-drain stops the very next
 * batch. A failed read ends the cycle at once, so an outage costs at most one check-timeout per poll.
 */
public class MatcherLagGate {

    /** Logs why the lag could not be read; the gate's state itself is logged once per cycle by the evaluator. */
    private static final Logger log = LoggerFactory.getLogger(MatcherLagGate.class);

    /** Whether a cycle may run, and if not, why. */
    public enum GateDecision {
        /** The Matcher Service is caught up: run the cycle. */
        PROCESS("none"),
        /** The Matcher Service is behind: lag went above pause-above and has not yet fallen to resume-at-or-below. */
        PAUSED_LAG("lag"),
        /** The lag could not be read, so the gate assumes the Matcher Service is behind (fail-closed). */
        PAUSED_UNAVAILABLE("unavailable"),
        /** Held by {@code force-pause}, whatever the lag. */
        PAUSED_FORCED("forced");

        /** Value of the {@code reason} tag on {@code cce.sla.evaluator.cycles.skipped}. */
        private final String skipReasonTag;

        /**
         * Pairs the decision with its metric tag value.
         *
         * @param skipReasonTag value of the {@code reason} tag when this decision skips a cycle
         */
        GateDecision(String skipReasonTag) {
            this.skipReasonTag = skipReasonTag;
        }

        /**
         * Whether the evaluator may run its cycle or next batch.
         *
         * @return true only for {@link #PROCESS}; every other decision holds the cycle back
         */
        public boolean allowsSlaEvaluation() {
            return this == PROCESS;
        }

        /**
         * The value the evaluator tags {@code cce.sla.evaluator.cycles.skipped} with.
         *
         * @return the metric tag value for a cycle this decision skipped
         */
        public String skipReasonTag() {
            return skipReasonTag;
        }
    }

    /** Thresholds, timeouts and switches from {@code cce.sla.matcher-lag-gate.*}. */
    private final MatcherLagGateProperties gateProperties;

    /** Reads the lag from Kafka; null when the gate is disabled. */
    private final MatcherLagReader lagReader;

    /** {@code cce.sla.matcher.lag.partition}: one gauge row per partition, replaced on every check. */
    private final MultiGauge partitionLagGauge;

    /**
     * Hysteresis state: true from the moment lag goes above pause-above (or cannot be read) until it is at
     * or below resume-at-or-below. Starts true so the service starts paused.
     */
    private boolean pausedUntilLagDrains = true;

    /**
     * The most recent decision, for the per-cycle state line and the paused gauge. Volatile because the
     * metrics scrape reads it from another thread.
     */
    private volatile GateDecision lastGateDecision;

    /** Total lag at the last check, for the {@code cce.sla.matcher.lag} gauge; NaN when unreadable. */
    private volatile double lastTotalLag = Double.NaN;

    /**
     * Creates the gate and registers its three gauges.
     *
     * @param gateProperties thresholds, timeouts and switches
     * @param lagReader      reads the lag from Kafka; null when the gate is disabled, which is the only case
     *                       it is never called
     * @param meterRegistry  where the lag and paused gauges are registered
     */
    public MatcherLagGate(MatcherLagGateProperties gateProperties, MatcherLagReader lagReader,
                          MeterRegistry meterRegistry) {
        this.gateProperties = gateProperties;
        this.lagReader = lagReader;
        // Total lag, read from the last check rather than by querying Kafka on every scrape.
        Gauge.builder("cce.sla.matcher.lag", this, lagGate -> lagGate.lastTotalLag)
                .description("Total messages the Matcher Service's consumer group has yet to process, as of "
                        + "the last check; NaN when it could not be read")
                .register(meterRegistry);
        // Per-partition lag, to tell one patient's burst from a broad backlog when the gate closes.
        this.partitionLagGauge = MultiGauge.builder("cce.sla.matcher.lag.partition")
                .description("Messages the Matcher Service has yet to process, per partition, as of the "
                        + "last check")
                .register(meterRegistry);
        // A yes/no flag as a number: 1 = paused (for any reason), 0 = running. The signal to alert on when a
        // pause lasts too long, e.g. cce_sla_gate_paused == 1 for 30 minutes.
        Gauge.builder("cce.sla.gate.paused", this, lagGate -> lagGate.isSlaEvaluationPaused() ? 1 : 0)
                .description("Whether the Matcher lag gate is holding SLA evaluation back: 1 = paused, 0 = running")
                .register(meterRegistry);
    }

    /**
     * Whether a cycle, or the next batch of one, may run now.
     *
     * <p>Reads the lag from Kafka on every call. Synchronized because the pause state is read and written
     * together; in practice only the single scheduler thread calls it.
     *
     * @return {@link GateDecision#PROCESS} to run, or the reason to hold back
     */
    public synchronized GateDecision decideWhetherSlaEvaluationMayRun() {
        // Disabled: always run, and never contact Kafka.
        if (!gateProperties.enabled()) {
            return GateDecision.PROCESS;
        }
        // Read the lag now and decide.
        GateDecision gateDecision = readLagAndDecide();
        lastGateDecision = gateDecision;
        return gateDecision;
    }

    /**
     * Describes the gate's current state in one sentence, for the line the evaluator logs on every scheduled
     * cycle — running or paused.
     *
     * <p>Running: the lag and the threshold that would pause it. Paused: why, the lag at the last check and
     * the threshold it is waiting on — enough to tell from successive log lines whether the Matcher Service
     * is draining, stuck or unreachable. When a pause began is the timestamp of the first "paused" line.
     *
     * @return the description, starting with {@code running}, {@code paused}, {@code disabled} or
     *         {@code not checked yet}
     */
    public synchronized String describeCurrentGateState() {
        if (!gateProperties.enabled()) {
            // e.g. "disabled — SLA evaluation runs regardless of Matcher Service lag"
            return "disabled — SLA evaluation runs regardless of Matcher Service lag";
        }
        GateDecision latestGateDecision = lastGateDecision;
        if (latestGateDecision == null) {
            return "not checked yet";
        }
        String lagReading = Double.isNaN(lastTotalLag) ? "unknown" : (long) lastTotalLag + " message(s)";
        if (latestGateDecision.allowsSlaEvaluation()) {
            // e.g. "running — Matcher Service lag is 3 message(s), pauses if it goes above 100"
            return "running — Matcher Service lag is " + lagReading + ", pauses if it goes above "
                    + gateProperties.pauseAbove();
        }
        String pauseReason = switch (latestGateDecision) {
            // e.g. "Matcher Service lag is 512 message(s) on group cce-matcher-service, topic
            // cce.events.inbound — paused once lag went above 100, resumes when it is at or below 5"
            case PAUSED_LAG -> "Matcher Service lag is " + lagReading + " on group "
                    + gateProperties.groupId() + ", topic " + gateProperties.topic()
                    + " — paused once lag went above " + gateProperties.pauseAbove()
                    + ", resumes when it is at or below " + gateProperties.resumeAtOrBelow();
            // e.g. "Matcher Service lag could not be read from Kafka (kafka:9092, group cce-matcher-service,
            // topic cce.events.inbound) — paused until it can be read and is at or below 5"
            case PAUSED_UNAVAILABLE -> "Matcher Service lag could not be read from Kafka ("
                    + gateProperties.bootstrapServers() + ", group " + gateProperties.groupId() + ", topic "
                    + gateProperties.topic() + ") — paused until it can be read and is at or below "
                    + gateProperties.resumeAtOrBelow();
            // e.g. "cce.sla.matcher-lag-gate.force-pause is set (Matcher Service lag is 3 message(s)) — paused
            // until it is unset"
            default -> "cce.sla.matcher-lag-gate.force-pause is set (Matcher Service lag is " + lagReading
                    + ") — paused until it is unset";
        };
        return "paused — " + pauseReason;
    }


    /**
     * Reads the lag and applies the rules: fail-closed on an unreadable lag, pause above pause-above,
     * resume only at or below resume-at-or-below, and force-pause over everything.
     *
     * @return the decision for this check
     */
    private GateDecision readLagAndDecide() {
        MatcherLag matcherLag;
        try {
            matcherLag = lagReader.readLagPerPartition();
        } catch (RuntimeException e) {
            // Fail-closed: treat an unreadable lag as a backlog, so recovering also needs the lag to drain.
            pausedUntilLagDrains = true;
            // Clear the lag metrics rather than leave the last good reading looking current.
            lastTotalLag = Double.NaN;
            partitionLagGauge.register(List.of(), true);
            GateDecision gateDecision = gateProperties.forcePause()
                    ? GateDecision.PAUSED_FORCED
                    : GateDecision.PAUSED_UNAVAILABLE;
            // The cycle's own log line says the lag could not be read; this says why. Message only, no stack
            // trace, since it repeats on every cycle while Kafka stays unreachable.
            // e.g. "Could not read the Matcher Service lag: Timed out trying to describe topic cce.events.inbound"
            log.warn("Could not read the Matcher Service lag: {}{}", e.getMessage(),
                    e.getCause() == null ? "" : " (cause: " + e.getCause() + ")");
            return gateDecision;
        }

        long totalLag = matcherLag.totalLag();
        publishLagToGauges(matcherLag, totalLag);
        // Hysteresis: a paused gate reopens only once the lag has drained to resume-at-or-below; an open
        // gate closes only once it rises above pause-above. In between, the gate keeps its current state.
        if (pausedUntilLagDrains && totalLag <= gateProperties.resumeAtOrBelow()) {
            pausedUntilLagDrains = false;
        } else if (!pausedUntilLagDrains && totalLag > gateProperties.pauseAbove()) {
            pausedUntilLagDrains = true;
        }

        // force-pause wins over the lag; the lag was still read above so its metrics stay current.
        return gateProperties.forcePause() ? GateDecision.PAUSED_FORCED
                : pausedUntilLagDrains ? GateDecision.PAUSED_LAG
                : GateDecision.PROCESS;
    }

    /**
     * Publishes a successful reading to the lag gauges.
     *
     * @param matcherLag the reading, per partition
     * @param totalLag   its total, already computed by the caller
     */
    private void publishLagToGauges(MatcherLag matcherLag, long totalLag) {
        lastTotalLag = totalLag;
        // overwrite = true replaces every row, so a partition that disappears stops being reported.
        partitionLagGauge.register(matcherLag.lagByPartition().entrySet().stream()
                .map(partitionLag -> MultiGauge.Row.of(
                        Tags.of("partition", String.valueOf(partitionLag.getKey())), partitionLag.getValue()))
                .toList(), true);
    }

    /**
     * Whether evaluation is currently held back, from the last decision.
     *
     * @return true while paused for any reason, for the paused gauge
     */
    private boolean isSlaEvaluationPaused() {
        GateDecision latestGateDecision = lastGateDecision;
        if (latestGateDecision == null) {
            // No check yet: paused if the gate is on, since it starts closed.
            return gateProperties.enabled();
        }
        return !latestGateDecision.allowsSlaEvaluation();
    }
}
