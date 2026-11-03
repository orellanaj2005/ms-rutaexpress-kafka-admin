package cl.rutaexpress.kafkaadmin.dto;

import java.util.List;

/** Lag real de un grupo (GET /api/kafka/consumer-groups/{id}/lag). */
public record ConsumerGroupLag(
        String groupId,
        String state,
        long totalLag,
        List<PartitionLag> partitions
) {
}
