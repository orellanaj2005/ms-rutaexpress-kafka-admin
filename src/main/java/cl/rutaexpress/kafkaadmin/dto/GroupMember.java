package cl.rutaexpress.kafkaadmin.dto;

import java.util.List;

/**
 * Un consumidor activo dentro del grupo. Con concurrencia 3 cada microservicio aparece
 * como 3 miembros (una hebra por partición).
 */
public record GroupMember(
        String memberId,
        String clientId,
        String host,
        List<TopicPartitionRef> assignments
) {
}
