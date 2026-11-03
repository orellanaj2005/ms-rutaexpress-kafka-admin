package cl.rutaexpress.kafkaadmin.dto;

import java.util.List;

/** Detalle de un tópico (GET /api/kafka/topics/{name}). */
public record TopicDetail(
        String name,
        String topicId,
        boolean internal,
        boolean protectedTopic,
        int partitionCount,
        int replicationFactor,
        List<PartitionDetail> partitions
) {
}
