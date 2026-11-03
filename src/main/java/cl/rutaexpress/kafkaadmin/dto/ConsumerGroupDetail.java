package cl.rutaexpress.kafkaadmin.dto;

import java.util.List;

/** Detalle de un consumer group (GET /api/kafka/consumer-groups/{id}). */
public record ConsumerGroupDetail(
        String groupId,
        String state,
        String partitionAssignor,
        Integer coordinator,
        boolean protectedGroup,
        List<GroupMember> members
) {
}
