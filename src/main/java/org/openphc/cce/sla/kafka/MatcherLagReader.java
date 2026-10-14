package org.openphc.cce.sla.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reads how far the Matcher Service's consumer group is behind on its topic, per partition.
 *
 * <p>Lag on a partition is its end offset (where events have reached) minus the offset the Matcher Service
 * has committed there (where it has reached). It uses the Kafka admin API only: both are metadata, so no consumer
 * joins the group and nothing is read from the topic. The Matcher Service commits an offset only after a
 * record's database writes are committed ({@code enable-auto-commit: false}, {@code ack-mode: record}), so
 * a lag of zero means its writes for every event are done, not merely that the records were fetched.
 *
 * <p>Three choices keep the number honest:
 * <ul>
 *   <li>The partition list is described on every read rather than cached, so a topic that gains
 *       partitions is measured in full from the next check.</li>
 *   <li>End offsets are read-committed, because the Matcher Service consumes with
 *       {@code isolation.level: read_committed} and can never get past the last stable offset. Nothing
 *       writes to the topic transactionally today, so this equals the high watermark; it stops an open
 *       transaction ever reading as backlog.</li>
 *   <li>A partition with no committed offset counts from zero. That can only overstate the lag — by events
 *       retention has already deleted — never understate it, so the gate errs towards staying paused. It
 *       corrects itself once the Matcher Service commits on that partition.</li>
 * </ul>
 *
 * <p>Any failure — broker unreachable, unknown topic, the time budget spent — is thrown, never turned into
 * a number. Deciding what an unreadable lag means is {@link MatcherLagGate}'s job.
 */
public class MatcherLagReader {

    /** Warns when the consumer group has no committed offsets at all, which usually means a wrong group-id. */
    private static final Logger log = LoggerFactory.getLogger(MatcherLagReader.class);

    /** Kafka admin client used for the metadata reads; owned and closed by the Spring context. */
    private final Admin kafkaAdminClient;

    /** The Matcher Service's consumer group, whose committed offsets mark how far it has processed. */
    private final String matcherConsumerGroupId;

    /** The topic the Matcher Service consumes, whose end offsets mark how far events have arrived. */
    private final String matcherTopicName;

    /**
     * Budget for one whole {@link #readLagPerPartition()}, shared across its Kafka admin calls rather than given
     * to each.
     */
    private final Duration lagCheckTimeout;

    /**
     * Creates a reader for one consumer group on one topic.
     *
     * @param kafkaAdminClient       Kafka admin client to read metadata through
     * @param matcherConsumerGroupId the Matcher Service's consumer group
     * @param matcherTopicName       the topic that group consumes
     * @param lagCheckTimeout        the longest one {@link #readLagPerPartition()} may take before it fails
     */
    public MatcherLagReader(Admin kafkaAdminClient, String matcherConsumerGroupId, String matcherTopicName,
                            Duration lagCheckTimeout) {
        this.kafkaAdminClient = kafkaAdminClient;
        this.matcherConsumerGroupId = matcherConsumerGroupId;
        this.matcherTopicName = matcherTopicName;
        this.lagCheckTimeout = lagCheckTimeout;
    }

    /**
     * Reads the Matcher Service's lag on every partition of the topic, in three steps: where events have
     * reached, where the Matcher Service has reached, and the difference.
     *
     * <p>Makes three Kafka admin calls, all within one {@code lagCheckTimeout}.
     *
     * @return lag per partition number, every partition of the topic included
     * @throws MatcherLagUnavailableException if any part of the read fails or the time budget runs out
     */
    public MatcherLag readLagPerPartition() {
        // One deadline for the whole read, so three slow calls cannot add up to three timeouts.
        // e.g. with a 5s lag-check timeout, if describing the topic takes 4s, the remaining calls share the
        // last 1s rather than getting 5s each — the whole read never takes more than 5s.
        long deadlineNanos = System.nanoTime() + lagCheckTimeout.toNanos();

        // 1. Where events have reached on each partition.
        //    e.g. {p0=10450, p1=9870, p2=11212, ..., p24=9993}
        Map<TopicPartition, Long> partitionEndOffsets = fetchPartitionEndOffsets(deadlineNanos);

        // 2. Where the Matcher Service has reached on each partition.
        //    e.g. {p0=10450, p1=9870, p2=11200, ..., p24=9990}
        Map<TopicPartition, Long> matcherCommittedOffsets = fetchMatcherCommittedOffsets(deadlineNanos);

        // 3. The difference per partition; MatcherLag.totalLag() adds them up for the gate.
        //    e.g. {0=0, 1=0, 2=12, ..., 24=3}, total lag 15
        return calculateLagPerPartition(partitionEndOffsets, matcherCommittedOffsets);
    }

    /**
     * Step 1: lists every partition of the topic and its read-committed end offset — how far events have
     * arrived. Two Kafka admin calls: describe the topic, then list the end offsets of its partitions.
     *
     * @param deadlineNanos {@link System#nanoTime()} value by which both calls must answer
     * @return end offset per partition, every partition of the topic included
     */
    private Map<TopicPartition, Long> fetchPartitionEndOffsets(long deadlineNanos) {
        // Which partitions the topic has now. Described every time, so added partitions are counted.
        // e.g. [cce.events.inbound-0, cce.events.inbound-1, ..., cce.events.inbound-24] for 25 partitions.
        List<TopicPartition> topicPartitions = awaitKafkaAdminCallWithinDeadline(
                kafkaAdminClient.describeTopics(List.of(matcherTopicName),
                                new DescribeTopicsOptions().timeoutMs(remainingMillisUntilDeadline(deadlineNanos)))
                        .allTopicNames(),
                deadlineNanos, "describe topic " + matcherTopicName)
                .get(matcherTopicName).partitions().stream()
                .map(partitionInfo -> new TopicPartition(matcherTopicName, partitionInfo.partition()))
                .toList();

        // listOffsets needs to be told, per partition, which offset to return (an OffsetSpec): latest = the
        // end of the log, earliest = the oldest event still kept, forTimestamp = the first event at or after a
        // time. Lag needs only the end, so every partition asks for latest.
        // e.g. {p0=latest, p1=latest, ..., p24=latest}
        Map<TopicPartition, OffsetSpec> latestOffsetSpecByPartition = new HashMap<>();
        topicPartitions.forEach(topicPartition -> latestOffsetSpecByPartition.put(topicPartition, OffsetSpec.latest()));

        // Ask Kafka for each partition's end offset — read-committed, matching how the Matcher Service
        // consumes — and keep just the offset number from each answer (Kafka also returns a timestamp and
        // leader epoch that lag does not need).
        // e.g. {p0=ListOffsetsResultInfo(offset=10450, ...), ...} becomes {p0=10450, p1=9870, ..., p24=9993}
        Map<TopicPartition, Long> partitionEndOffsets = new HashMap<>();
        awaitKafkaAdminCallWithinDeadline(
                kafkaAdminClient.listOffsets(latestOffsetSpecByPartition,
                                new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)
                                        .timeoutMs(remainingMillisUntilDeadline(deadlineNanos)))
                        .all(),
                deadlineNanos, "list end offsets of " + matcherTopicName)
                .forEach((topicPartition, endOffsetInfo) ->
                        partitionEndOffsets.put(topicPartition, endOffsetInfo.offset()));
        return partitionEndOffsets;
    }

    /**
     * Step 2: lists the offset the Matcher Service's consumer group last committed on each partition — how
     * far it has processed, database writes included. One Kafka admin call.
     *
     * @param deadlineNanos {@link System#nanoTime()} value by which the call must answer
     * @return committed offset per partition; a partition the group has never committed on is absent
     */
    private Map<TopicPartition, Long> fetchMatcherCommittedOffsets(long deadlineNanos) {
        Map<TopicPartition, OffsetAndMetadata> committedOffsetsWithMetadata = awaitKafkaAdminCallWithinDeadline(
                kafkaAdminClient.listConsumerGroupOffsets(matcherConsumerGroupId,
                                new ListConsumerGroupOffsetsOptions()
                                        .timeoutMs(remainingMillisUntilDeadline(deadlineNanos)))
                        .partitionsToOffsetAndMetadata(),
                deadlineNanos, "list committed offsets of group " + matcherConsumerGroupId);

        // Kafka may list a partition without a commit as null rather than leave it out; drop those, so
        // "never committed" always reads as absent.
        // e.g. {p0=OffsetAndMetadata(10450), p7=null} becomes {p0=10450}
        Map<TopicPartition, Long> matcherCommittedOffsets = new HashMap<>();
        committedOffsetsWithMetadata.forEach((topicPartition, offsetAndMetadata) -> {
            if (offsetAndMetadata != null) {
                matcherCommittedOffsets.put(topicPartition, offsetAndMetadata.offset());
            }
        });
        return matcherCommittedOffsets;
    }

    /**
     * Step 3: subtracts, per partition, where the Matcher Service has reached from where events have reached.
     *
     * @param partitionEndOffsets     end offset per partition, from step 1
     * @param matcherCommittedOffsets committed offset per partition, from step 2
     * @return lag per partition number; {@link MatcherLag#totalLag()} gives the total
     */
    private MatcherLag calculateLagPerPartition(Map<TopicPartition, Long> partitionEndOffsets,
                                                Map<TopicPartition, Long> matcherCommittedOffsets) {
        if (!partitionEndOffsets.isEmpty() && matcherCommittedOffsets.isEmpty()) {
            // Either the Matcher Service has never consumed, or group-id names the wrong group. Both mean
            // the whole topic counts as backlog, so the gate stays shut — say which group it asked about so
            // a typo is quick to find.
            // e.g. group-id set to "cce-matcher-servce": no partition has a commit, every partition counts
            // from zero, the lag is the whole topic, and this warning names the typo.
            log.warn("Consumer group {} has no committed offset on any partition of {} — counting every "
                    + "partition from zero. Check cce.sla.matcher-lag-gate.group-id if the Matcher Service is "
                    + "running.", matcherConsumerGroupId, matcherTopicName);
        }

        // TreeMap keeps partitions in numeric order for logs and metrics.
        Map<Integer, Long> lagByPartition = new TreeMap<>();
        partitionEndOffsets.forEach((topicPartition, partitionEndOffset) -> {
            // A partition with no commit counts from zero: that can only overstate the lag, never hide it.
            // e.g. a newly added p25 with end offset 40 and no commit yet: 40 - 0 = 40.
            long matcherCommittedOffset = matcherCommittedOffsets.getOrDefault(topicPartition, 0L);
            // Clamped at zero: a commit can briefly sit past the end offset read a moment earlier.
            // e.g. end offset read as 9990, then the Matcher Service commits 9992: -2 is recorded as 0.
            // e.g. p2: 11212 - 11200 = 12.
            lagByPartition.put(topicPartition.partition(),
                    Math.max(0, partitionEndOffset - matcherCommittedOffset));
        });
        return new MatcherLag(lagByPartition);
    }

    /**
     * Waits for a Kafka admin call's result, but no later than the deadline, and turns every way it can fail
     * into one {@link MatcherLagUnavailableException}.
     *
     * @param pendingKafkaAdminCallResult the result of a Kafka admin call that may not have arrived yet
     * @param deadlineNanos               {@link System#nanoTime()} value after which to give up
     * @param kafkaAdminCallDescription   what the call does, for the exception message
     * @param <T>                         the call's result type
     * @return the call's result
     * @throws MatcherLagUnavailableException if the call fails, times out or the thread is interrupted
     */
    private static <T> T awaitKafkaAdminCallWithinDeadline(KafkaFuture<T> pendingKafkaAdminCallResult,
                                                           long deadlineNanos,
                                                           String kafkaAdminCallDescription) {
        try {
            // Wait only for whatever is left of the shared deadline; zero if it has already passed.
            // e.g. deadline at 5.0s, now 4.2s: wait at most 0.8s for this call's result.
            long nanosLeftUntilDeadline = Math.max(0, deadlineNanos - System.nanoTime());
            return pendingKafkaAdminCallResult.get(nanosLeftUntilDeadline, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            // Keep the interrupt visible to the caller (e.g. a shutting-down scheduler) rather than swallow it.
            // e.g. the service is stopping mid-check: the read fails with "Interrupted during describe topic
            // cce.events.inbound" and the scheduler still sees the interrupt and can shut down.
            Thread.currentThread().interrupt();
            throw new MatcherLagUnavailableException("Interrupted during " + kafkaAdminCallDescription, e);
        } catch (ExecutionException e) {
            // The broker answered with an error; its cause (e.g. unknown topic) is the useful part.
            // e.g. topic misconfigured as "cce.events.inbnd": "Could not describe topic cce.events.inbnd",
            // caused by UnknownTopicOrPartitionException.
            throw new MatcherLagUnavailableException("Could not " + kafkaAdminCallDescription, e.getCause());
        } catch (TimeoutException e) {
            // No answer before the deadline.
            // e.g. broker down: "Timed out trying to describe topic cce.events.inbound" after 5s.
            throw new MatcherLagUnavailableException("Timed out trying to " + kafkaAdminCallDescription, e);
        }
    }

    /**
     * Milliseconds left until the deadline, for a Kafka admin call's own {@code timeoutMs} option.
     *
     * <p>At least 1ms: the Kafka admin client reads zero as "use the default", which is the opposite of intended.
     *
     * @param deadlineNanos {@link System#nanoTime()} value of the deadline
     * @return the remaining time in milliseconds, never below 1
     */
    private static int remainingMillisUntilDeadline(long deadlineNanos) {
        // e.g. 0.8s left gives 800; a deadline already passed gives 1, not 0 (which means "client default").
        return (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
    }

    /**
     * The result of one read: lag per partition number, and the total the gate decides on.
     *
     * @param lagByPartition messages not yet processed, keyed by partition number
     */
    public record MatcherLag(Map<Integer, Long> lagByPartition) {

        /** Copies the map so a caller cannot change a reading after the fact. */
        public MatcherLag {
            lagByPartition = Map.copyOf(lagByPartition);
        }

        /**
         * Sums the lag of every partition.
         *
         * @return messages not yet processed across all partitions — the number the gate compares against
         *         its thresholds
         */
        public long totalLag() {
            // e.g. {0=0, 1=0, 2=12, ..., 24=3} gives 15.
            return lagByPartition.values().stream().mapToLong(Long::longValue).sum();
        }
    }

    /** The lag could not be read. The gate treats this as "assume the Matcher Service is behind". */
    public static class MatcherLagUnavailableException extends RuntimeException {

        /**
         * Wraps a failed lag read.
         *
         * @param message what was being attempted when it failed
         * @param cause   the underlying Kafka or timeout error
         */
        public MatcherLagUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
