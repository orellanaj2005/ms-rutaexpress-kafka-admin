package cl.rutaexpress.kafkaadmin.dto;

import java.util.List;

/** Respuesta de POST /api/kafka/consumer-groups/{id}/reset. */
public record ResetOffsetsResponse(
        String groupId,
        OffsetResetStrategy strategy,
        List<PartitionOffsetChange> partitions
) {
}
