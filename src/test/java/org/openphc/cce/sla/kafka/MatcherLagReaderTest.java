package org.openphc.cce.sla.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.openphc.cce.sla.kafka.MatcherLagReader.MatcherLag;
import org.openphc.cce.sla.kafka.MatcherLagReader.MatcherLagUnavailableException;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link MatcherLagReader} against a mocked Kafka {@link Admin} client: how lag is computed
 * from committed, end and log-start offsets, and how each kind of failure is reported.
 */
class MatcherLagReaderTest {

    private static final String TOPIC_NAME = "cce.events.inbound";
    private static final String CONSUMER_GROUP_ID = "cce-matcher-service";

    private final Admin kafkaAdminClient = mock(Admin.class);
    private final MatcherLagReader lagReader =
            new MatcherLagReader(kafkaAdminClient, CONSUMER_GROUP_ID, TOPIC_NAME, Duration.ofSeconds(5));

    /** A partition of the test topic by number. */
    private static TopicPartition topicPartition(int partitionNumber) {
        return new TopicPartition(TOPIC_NAME, partitionNumber);
    }

    /** Makes describe-topics report the test topic with this many partitions. */
    private void stubTopicWithPartitions(int partitionCount) {
        List<TopicPartitionInfo> partitionInfos = IntStream.range(0, partitionCount)
                .mapToObj(partitionNumber -> new TopicPartitionInfo(partitionNumber, null, List.of(), List.of()))
                .toList();
        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(describeTopicsResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(
                Map.of(TOPIC_NAME, new TopicDescription(TOPIC_NAME, false, partitionInfos))));
        when(kafkaAdminClient.describeTopics(eq(List.of(TOPIC_NAME)), any())).thenReturn(describeTopicsResult);
    }

    /** Makes describe-topics return this future, to simulate a failed or never-answered call. */
    private void stubDescribeTopicsFuture(KafkaFuture<Map<String, TopicDescription>> describeTopicsFuture) {
        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(describeTopicsResult.allTopicNames()).thenReturn(describeTopicsFuture);
        when(kafkaAdminClient.describeTopics(anyCollection(), any())).thenReturn(describeTopicsResult);
    }

    /** Makes the consumer group's committed offsets these. */
    private void stubCommittedOffsets(Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
        ListConsumerGroupOffsetsResult groupOffsetsResult = mock(ListConsumerGroupOffsetsResult.class);
        when(groupOffsetsResult.partitionsToOffsetAndMetadata())
                .thenReturn(KafkaFuture.completedFuture(committedOffsets));
        when(kafkaAdminClient.listConsumerGroupOffsets(eq(CONSUMER_GROUP_ID), any())).thenReturn(groupOffsetsResult);
    }

    /** Makes list-offsets return these end offsets. */
    private void stubPartitionEndOffsets(Map<TopicPartition, Long> endOffsetByPartition) {
        Map<TopicPartition, ListOffsetsResultInfo> resultInfoByPartition = new HashMap<>();
        endOffsetByPartition.forEach((topicPartition, endOffset) ->
                resultInfoByPartition.put(topicPartition, new ListOffsetsResultInfo(endOffset, -1, Optional.empty())));
        ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
        when(listOffsetsResult.all()).thenReturn(KafkaFuture.completedFuture(resultInfoByPartition));
        when(kafkaAdminClient.listOffsets(anyMap(), any())).thenReturn(listOffsetsResult);
    }

    /** Lag on each partition is its end offset minus the group's committed offset. */
    @Test
    void lagIsEndOffsetMinusCommittedOffset_perPartition() {
        stubTopicWithPartitions(2);
        stubCommittedOffsets(Map.of(topicPartition(0), new OffsetAndMetadata(40),
                topicPartition(1), new OffsetAndMetadata(10)));
        stubPartitionEndOffsets(Map.of(topicPartition(0), 42L, topicPartition(1), 10L));

        MatcherLag matcherLag = lagReader.readLagPerPartition();

        assertEquals(Map.of(0, 2L, 1, 0L), matcherLag.lagByPartition());
        assertEquals(2, matcherLag.totalLag());
    }

    /** End offsets are requested with read-committed isolation. */
    @Test
    void endOffsetsAreReadCommitted_matchingHowTheMatcherConsumes() {
        // The Matcher Service can never read past the last stable offset, so an open transaction is not
        // backlog it has yet to process.
        stubTopicWithPartitions(1);
        stubCommittedOffsets(Map.of(topicPartition(0), new OffsetAndMetadata(0)));
        stubPartitionEndOffsets(Map.of(topicPartition(0), 0L));

        lagReader.readLagPerPartition();

        ArgumentCaptor<ListOffsetsOptions> listOffsetsOptions = ArgumentCaptor.forClass(ListOffsetsOptions.class);
        verify(kafkaAdminClient).listOffsets(anyMap(), listOffsetsOptions.capture());
        assertEquals(IsolationLevel.READ_COMMITTED, listOffsetsOptions.getValue().isolationLevel());
    }

    /** A partition with no commit counts from zero, so its whole log counts as lag. */
    @Test
    void partitionWithoutACommittedOffset_countsFromZero() {
        // Can only overstate the lag, never hide it: the gate errs towards staying paused.
        stubTopicWithPartitions(2);
        stubCommittedOffsets(Map.of(topicPartition(0), new OffsetAndMetadata(5)));
        stubPartitionEndOffsets(Map.of(topicPartition(0), 5L, topicPartition(1), 40L));

        assertEquals(Map.of(0, 0L, 1, 40L), lagReader.readLagPerPartition().lagByPartition());
    }

    /** A partition Kafka lists with a null commit is treated the same as one it leaves out. */
    @Test
    void partitionListedWithANullCommit_countsFromZero() {
        stubTopicWithPartitions(1);
        Map<TopicPartition, OffsetAndMetadata> committedWithNull = new HashMap<>();
        committedWithNull.put(topicPartition(0), null);
        stubCommittedOffsets(committedWithNull);
        stubPartitionEndOffsets(Map.of(topicPartition(0), 7L));

        assertEquals(7, lagReader.readLagPerPartition().totalLag());
    }

    /** A group with no commits at all counts every partition's whole log as lag. */
    @Test
    void groupWithNoCommittedOffsets_countsTheWholeTopic() {
        // A group that has never consumed — or a mistyped group-id — must hold the gate shut, not open it.
        stubTopicWithPartitions(2);
        stubCommittedOffsets(Map.of());
        stubPartitionEndOffsets(Map.of(topicPartition(0), 30L, topicPartition(1), 20L));

        assertEquals(50, lagReader.readLagPerPartition().totalLag());
    }

    /** Only end offsets are listed — one list-offsets call per read, asking for the latest offset. */
    @Test
    void onlyEndOffsetsAreListed() {
        stubTopicWithPartitions(2);
        stubCommittedOffsets(Map.of());
        stubPartitionEndOffsets(Map.of(topicPartition(0), 1L, topicPartition(1), 1L));

        lagReader.readLagPerPartition();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetSpec>> offsetSpecByPartition = ArgumentCaptor.forClass(Map.class);
        verify(kafkaAdminClient, times(1)).listOffsets(offsetSpecByPartition.capture(), any());
        assertTrue(offsetSpecByPartition.getValue().values().stream()
                .allMatch(offsetSpec -> offsetSpec instanceof OffsetSpec.LatestSpec));
    }

    /** A commit past the end offset reads as zero lag, never negative. */
    @Test
    void committedAheadOfTheEnd_readsAsZeroRatherThanNegative() {
        stubTopicWithPartitions(1);
        stubCommittedOffsets(Map.of(topicPartition(0), new OffsetAndMetadata(12)));
        stubPartitionEndOffsets(Map.of(topicPartition(0), 10L));

        assertEquals(0, lagReader.readLagPerPartition().totalLag());
    }

    /** A topic with no partitions has zero lag. */
    @Test
    void emptyTopicDescription_isZeroLag() {
        stubTopicWithPartitions(0);
        stubCommittedOffsets(Map.of());
        stubPartitionEndOffsets(Map.of());

        assertEquals(0, lagReader.readLagPerPartition().totalLag());
    }

    /** A failed Kafka admin call is rethrown as MatcherLagUnavailableException carrying the broker's error. */
    @Test
    void failedCall_isThrownWithItsCause() {
        KafkaFutureImpl<Map<String, TopicDescription>> failedDescribeFuture = new KafkaFutureImpl<>();
        failedDescribeFuture.completeExceptionally(new UnknownTopicOrPartitionException("no such topic"));
        stubDescribeTopicsFuture(failedDescribeFuture);

        MatcherLagUnavailableException thrown =
                assertThrows(MatcherLagUnavailableException.class, lagReader::readLagPerPartition);
        assertInstanceOf(UnknownTopicOrPartitionException.class, thrown.getCause());
    }

    /** A call that never answers fails at the check timeout instead of hanging. */
    @Test
    void brokerThatNeverAnswers_failsWithinTheTimeout() {
        // The scheduler thread is single; a hung broker must cost one check-timeout, not the client default.
        MatcherLagReader shortTimeoutReader =
                new MatcherLagReader(kafkaAdminClient, CONSUMER_GROUP_ID, TOPIC_NAME, Duration.ofMillis(50));
        stubDescribeTopicsFuture(new KafkaFutureImpl<>());

        long startNanos = System.nanoTime();
        assertThrows(MatcherLagUnavailableException.class, shortTimeoutReader::readLagPerPartition);
        assertTrue(Duration.ofNanos(System.nanoTime() - startNanos).compareTo(Duration.ofSeconds(2)) < 0);
    }

    /** An interrupted wait fails the read and leaves the thread's interrupt flag set. */
    @Test
    void interruption_isRethrownAndTheFlagRestored() {
        stubDescribeTopicsFuture(new KafkaFutureImpl<>());

        Thread.currentThread().interrupt();
        try {
            assertThrows(MatcherLagUnavailableException.class, lagReader::readLagPerPartition);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
