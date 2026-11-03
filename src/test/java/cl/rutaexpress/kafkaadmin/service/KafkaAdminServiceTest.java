package cl.rutaexpress.kafkaadmin.service;

import cl.rutaexpress.kafkaadmin.config.KafkaAdminProperties;
import cl.rutaexpress.kafkaadmin.dto.ConsumerGroupLag;
import cl.rutaexpress.kafkaadmin.dto.CreateTopicRequest;
import cl.rutaexpress.kafkaadmin.dto.IncreasePartitionsRequest;
import cl.rutaexpress.kafkaadmin.dto.OffsetResetStrategy;
import cl.rutaexpress.kafkaadmin.dto.PartitionLag;
import cl.rutaexpress.kafkaadmin.dto.ResetOffsetsRequest;
import cl.rutaexpress.kafkaadmin.dto.ResetOffsetsResponse;
import cl.rutaexpress.kafkaadmin.dto.TopicSummary;
import cl.rutaexpress.kafkaadmin.dto.UpdateTopicConfigsRequest;
import cl.rutaexpress.kafkaadmin.exception.GroupNotInactiveException;
import cl.rutaexpress.kafkaadmin.exception.InvalidOperationException;
import cl.rutaexpress.kafkaadmin.exception.KafkaUnavailableException;
import cl.rutaexpress.kafkaadmin.exception.ProtectedResourceException;
import cl.rutaexpress.kafkaadmin.exception.ResourceNotFoundException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pruebas del servicio con el AdminClient mockeado: no necesitan un clúster Kafka.
 */
class KafkaAdminServiceTest {

    private static final Node BROKER_1 = new Node(1, "kafka1", 29092);
    private static final Node BROKER_2 = new Node(2, "kafka2", 29092);
    private static final Node BROKER_3 = new Node(3, "kafka3", 29092);

    private AdminClient admin;
    private KafkaAdminService service;

    @BeforeEach
    void setUp() {
        admin = mock(AdminClient.class);
        KafkaAdminProperties properties = new KafkaAdminProperties(
                List.of("shipments.events", "audit.timeline", "notify.DLT", "audit.DLT", "report.DLT"),
                List.of("ms-rutaexpress-audit"),
                List.of("retention.ms", "cleanup.policy", "min.insync.replicas"),
                1);
        service = new KafkaAdminService(admin, properties);
    }

    // ================================================================ tópicos

    @Test
    void listTopicsMapsPartitionsReplicasAndRetention() {
        ListTopicsResult listResult = mock(ListTopicsResult.class);
        when(listResult.listings()).thenReturn(done(List.of(
                new TopicListing("shipments.events", null, false),
                new TopicListing("pruebas.demo", null, false))));
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(listResult);

        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(describeResult.allTopicNames()).thenReturn(done(Map.of(
                "shipments.events", topic("shipments.events", 3),
                "pruebas.demo", topic("pruebas.demo", 1))));
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);

        DescribeConfigsResult configsResult = mock(DescribeConfigsResult.class);
        when(configsResult.all()).thenReturn(done(Map.of(
                topicResource("shipments.events"), new Config(List.of(
                        new ConfigEntry("retention.ms", "604800000"),
                        new ConfigEntry("cleanup.policy", "delete"),
                        new ConfigEntry("min.insync.replicas", "2"))),
                topicResource("pruebas.demo"), new Config(List.of(
                        new ConfigEntry("retention.ms", "-1"))))));
        when(admin.describeConfigs(anyCollection())).thenReturn(configsResult);

        List<TopicSummary> topics = service.listTopics(false);

        assertEquals(2, topics.size());
        TopicSummary demo = topics.get(0);
        assertEquals("pruebas.demo", demo.name());
        assertEquals(-1L, demo.retentionMs());
        assertFalse(demo.protectedTopic());

        TopicSummary shipments = topics.get(1);
        assertEquals("shipments.events", shipments.name());
        assertEquals(3, shipments.partitions());
        assertEquals(3, shipments.replicationFactor());
        assertEquals(604800000L, shipments.retentionMs());
        assertEquals("delete", shipments.cleanupPolicy());
        assertEquals(2, shipments.minInsyncReplicas());
        assertTrue(shipments.protectedTopic());
    }

    @Test
    void createTopicRejectsReservedPrefixWithoutCallingKafka() {
        CreateTopicRequest request = new CreateTopicRequest("__mio", 3, 3, null);

        assertThrows(InvalidOperationException.class, () -> service.createTopic(request));
        verify(admin, never()).createTopics(anyCollection());
    }

    @Test
    void createTopicRejectsConfigsOutsideTheEditableList() {
        CreateTopicRequest request = new CreateTopicRequest("pruebas.demo", 3, 3, Map.of("unclean.leader.election.enable", "true"));

        assertThrows(InvalidOperationException.class, () -> service.createTopic(request));
        verify(admin, never()).createTopics(anyCollection());
    }

    @Test
    void createTopicRejectsMinInsyncReplicasGreaterThanReplicationFactor() {
        CreateTopicRequest request = new CreateTopicRequest("pruebas.demo", 3, 2, Map.of("min.insync.replicas", "3"));

        assertThrows(InvalidOperationException.class, () -> service.createTopic(request));
        verify(admin, never()).createTopics(anyCollection());
    }

    @SuppressWarnings("unchecked")
    @Test
    void createTopicWithOneReplicaLowersMinInsyncReplicasToOne() {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(done(null));
        when(admin.createTopics(anyCollection())).thenReturn(result);

        service.createTopic(new CreateTopicRequest("pruebas.demo", 2, 1, Map.of("retention.ms", "60000")));

        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createTopics(captor.capture());
        NewTopic created = captor.getValue().iterator().next();
        assertEquals("pruebas.demo", created.name());
        assertEquals(2, created.numPartitions());
        assertEquals((short) 1, created.replicationFactor());
        assertEquals("1", created.configs().get("min.insync.replicas"));
        assertEquals("60000", created.configs().get("retention.ms"));
    }

    @Test
    void createTopicTranslatesTopicExists() {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(failed(new TopicExistsException("Topic 'x' already exists.")));
        when(admin.createTopics(anyCollection())).thenReturn(result);

        TopicExistsException ex = assertThrows(TopicExistsException.class,
                () -> service.createTopic(new CreateTopicRequest("pruebas.demo", 3, 3, null)));
        assertTrue(ex.getMessage().contains("pruebas.demo"));
    }

    @Test
    void deleteTopicBlocksProtectedAndInternalTopics() {
        assertThrows(ProtectedResourceException.class, () -> service.deleteTopic("shipments.events"));
        assertThrows(ProtectedResourceException.class, () -> service.deleteTopic("__consumer_offsets"));
        verify(admin, never()).deleteTopics(anyCollection());
    }

    @Test
    void deleteTopicTranslatesUnknownTopicTo404() {
        DeleteTopicsResult result = mock(DeleteTopicsResult.class);
        when(result.all()).thenReturn(failed(new UnknownTopicOrPartitionException("unknown")));
        when(admin.deleteTopics(anyCollection())).thenReturn(result);

        assertThrows(ResourceNotFoundException.class, () -> service.deleteTopic("no.existe"));
    }

    @Test
    void increasePartitionsRejectsDecreasingOrEqualTotals() {
        stubDescribeTopic("shipments.events", 3);

        assertThrows(InvalidOperationException.class,
                () -> service.increasePartitions("shipments.events", new IncreasePartitionsRequest(3)));
        assertThrows(InvalidOperationException.class,
                () -> service.increasePartitions("shipments.events", new IncreasePartitionsRequest(2)));
        verify(admin, never()).createPartitions(anyMap());
    }

    @SuppressWarnings("unchecked")
    @Test
    void increasePartitionsAsksKafkaForTheNewTotal() {
        stubDescribeTopic("shipments.events", 3);
        CreatePartitionsResult result = mock(CreatePartitionsResult.class);
        when(result.all()).thenReturn(done(null));
        when(admin.createPartitions(anyMap())).thenReturn(result);

        var response = service.increasePartitions("shipments.events", new IncreasePartitionsRequest(6));

        ArgumentCaptor<Map<String, NewPartitions>> captor = ArgumentCaptor.forClass(Map.class);
        verify(admin).createPartitions(captor.capture());
        assertEquals(6, captor.getValue().get("shipments.events").totalCount());
        assertEquals(3, response.previousPartitions());
        assertEquals(6, response.partitions());
    }

    @SuppressWarnings("unchecked")
    @Test
    void updateTopicConfigsSendsSetAndDeleteOperations() {
        AlterConfigsResult result = mock(AlterConfigsResult.class);
        when(result.all()).thenReturn(done(null));
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(result);

        service.updateTopicConfigs("pruebas.demo", new UpdateTopicConfigsRequest(
                Map.of("cleanup.policy", "compact,delete"), List.of("retention.ms")));

        ArgumentCaptor<Map<ConfigResource, Collection<AlterConfigOp>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(admin).incrementalAlterConfigs(captor.capture());
        Collection<AlterConfigOp> ops = captor.getValue().get(topicResource("pruebas.demo"));
        assertEquals(2, ops.size());
        assertTrue(ops.stream().anyMatch(op -> op.opType() == AlterConfigOp.OpType.SET
                && op.configEntry().name().equals("cleanup.policy")
                && op.configEntry().value().equals("compact,delete")));
        assertTrue(ops.stream().anyMatch(op -> op.opType() == AlterConfigOp.OpType.DELETE
                && op.configEntry().name().equals("retention.ms")));
    }

    @Test
    void updateTopicConfigsValidatesMinInsyncReplicasAgainstTheTopic() {
        stubDescribeTopic("pruebas.demo", 3);

        assertThrows(InvalidOperationException.class, () -> service.updateTopicConfigs("pruebas.demo",
                new UpdateTopicConfigsRequest(Map.of("min.insync.replicas", "4"), null)));
        verify(admin, never()).incrementalAlterConfigs(anyMap());
    }

    @Test
    void updateTopicConfigsRejectsAnEmptyRequest() {
        assertThrows(InvalidOperationException.class,
                () -> service.updateTopicConfigs("pruebas.demo", new UpdateTopicConfigsRequest(null, null)));
    }

    // ======================================================= consumer groups

    @Test
    void lagIsEndOffsetMinusCommittedOffsetPerPartition() {
        TopicPartition p0 = new TopicPartition("shipments.events", 0);
        TopicPartition p1 = new TopicPartition("shipments.events", 1);
        MemberDescription member = new MemberDescription("consumer-1", "report-0", "/10.0.0.5",
                new MemberAssignment(Set.of(p0, p1)));
        stubDescribeGroup("ms-rutaexpress-report", ConsumerGroupState.STABLE, List.of(member));

        Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
        committed.put(p0, new OffsetAndMetadata(5));
        stubCommittedOffsets("ms-rutaexpress-report", committed);
        stubListOffsets(Map.of(p0, 12L, p1, 4L));

        ConsumerGroupLag lag = service.getConsumerGroupLag("ms-rutaexpress-report");

        assertEquals("STABLE", lag.state());
        assertEquals(7L, lag.totalLag());
        PartitionLag first = lag.partitions().get(0);
        assertEquals(0, first.partition());
        assertEquals(5L, first.committedOffset());
        assertEquals(12L, first.endOffset());
        assertEquals(7L, first.lag());
        assertEquals("report-0", first.clientId());

        PartitionLag second = lag.partitions().get(1);
        assertNull(second.committedOffset(), "partición asignada sin commit");
        assertNull(second.lag());
    }

    @Test
    void deadGroupIsReportedAsNotFound() {
        stubDescribeGroup("no-existe", ConsumerGroupState.DEAD, List.of());

        assertThrows(ResourceNotFoundException.class, () -> service.describeConsumerGroup("no-existe"));
    }

    @Test
    void resetOffsetsRequiresAnInactiveGroup() {
        MemberDescription member = new MemberDescription("consumer-1", "audit-0", "/10.0.0.5",
                new MemberAssignment(Set.of()));
        stubDescribeGroup("ms-rutaexpress-audit", ConsumerGroupState.STABLE, List.of(member));

        assertThrows(GroupNotInactiveException.class, () -> service.resetOffsets("ms-rutaexpress-audit",
                new ResetOffsetsRequest(OffsetResetStrategy.EARLIEST, null)));
        verify(admin, never()).alterConsumerGroupOffsets(anyString(), anyMap());
    }

    @SuppressWarnings("unchecked")
    @Test
    void resetOffsetsToEarliestCommitsTheBeginningOfEachPartition() {
        TopicPartition p0 = new TopicPartition("shipments.events", 0);
        TopicPartition p1 = new TopicPartition("shipments.events", 1);
        stubDescribeGroup("ms-rutaexpress-audit", ConsumerGroupState.EMPTY, List.of());
        stubCommittedOffsets("ms-rutaexpress-audit", Map.of(p0, new OffsetAndMetadata(40), p1, new OffsetAndMetadata(33)));
        stubListOffsets(Map.of(p0, 0L, p1, 0L));

        AlterConsumerGroupOffsetsResult alterResult = mock(AlterConsumerGroupOffsetsResult.class);
        when(alterResult.all()).thenReturn(done(null));
        when(admin.alterConsumerGroupOffsets(anyString(), anyMap())).thenReturn(alterResult);

        ResetOffsetsResponse response = service.resetOffsets("ms-rutaexpress-audit",
                new ResetOffsetsRequest(OffsetResetStrategy.EARLIEST, "shipments.events"));

        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
        verify(admin).alterConsumerGroupOffsets(eq("ms-rutaexpress-audit"), captor.capture());
        assertEquals(0L, captor.getValue().get(p0).offset());
        assertEquals(0L, captor.getValue().get(p1).offset());
        assertEquals(2, response.partitions().size());
        assertEquals(40L, response.partitions().get(0).previousOffset());

        ArgumentCaptor<Map<TopicPartition, OffsetSpec>> specCaptor = ArgumentCaptor.forClass(Map.class);
        verify(admin).listOffsets(specCaptor.capture());
        assertTrue(specCaptor.getValue().values().stream().allMatch(s -> s instanceof OffsetSpec.EarliestSpec));
    }

    @Test
    void deleteConsumerGroupBlocksProtectedGroups() {
        assertThrows(ProtectedResourceException.class, () -> service.deleteConsumerGroup("ms-rutaexpress-audit"));
        verify(admin, never()).deleteConsumerGroups(anyCollection());
    }

    @Test
    void aKafkaThatNeverAnswersSurfacesAsUnavailable() {
        ListTopicsResult listResult = mock(ListTopicsResult.class);
        when(listResult.listings()).thenReturn(new KafkaFutureImpl<>()); // nunca se completa
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(listResult);

        assertThrows(KafkaUnavailableException.class, () -> service.listTopics(false));
    }

    // =============================================================== helpers

    private static <T> KafkaFuture<T> done(T value) {
        return KafkaFuture.completedFuture(value);
    }

    private static <T> KafkaFuture<T> failed(Throwable error) {
        KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
        future.completeExceptionally(error);
        return future;
    }

    private static ConfigResource topicResource(String name) {
        return new ConfigResource(ConfigResource.Type.TOPIC, name);
    }

    private static TopicDescription topic(String name, int partitions) {
        List<TopicPartitionInfo> infos = new java.util.ArrayList<>();
        List<Node> replicas = List.of(BROKER_1, BROKER_2, BROKER_3);
        for (int i = 0; i < partitions; i++) {
            infos.add(new TopicPartitionInfo(i, replicas.get(i % 3), replicas, replicas));
        }
        return new TopicDescription(name, false, infos);
    }

    private void stubDescribeTopic(String name, int partitions) {
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        when(result.topicNameValues()).thenReturn(Map.of(name, done(topic(name, partitions))));
        when(admin.describeTopics(anyCollection())).thenReturn(result);
    }

    private void stubDescribeGroup(String groupId, ConsumerGroupState state, List<MemberDescription> members) {
        ConsumerGroupDescription description =
                new ConsumerGroupDescription(groupId, false, members, "range", state, BROKER_1);
        DescribeConsumerGroupsResult result = mock(DescribeConsumerGroupsResult.class);
        when(result.describedGroups()).thenReturn(Map.of(groupId, done(description)));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(result);
    }

    private void stubCommittedOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets) {
        ListConsumerGroupOffsetsResult result = mock(ListConsumerGroupOffsetsResult.class);
        when(result.partitionsToOffsetAndMetadata()).thenReturn(done(offsets));
        when(admin.listConsumerGroupOffsets(groupId)).thenReturn(result);
    }

    private void stubListOffsets(Map<TopicPartition, Long> offsets) {
        Map<TopicPartition, ListOffsetsResultInfo> infos = new HashMap<>();
        offsets.forEach((tp, offset) -> infos.put(tp, new ListOffsetsResultInfo(offset, -1L, Optional.empty())));
        ListOffsetsResult result = mock(ListOffsetsResult.class);
        when(result.all()).thenReturn(done(infos));
        when(admin.listOffsets(anyMap())).thenReturn(result);
    }
}
