package cl.rutaexpress.kafkaadmin.dto;

import java.util.Map;

/** Respuesta de POST /api/kafka/topics. */
public record TopicCreatedResponse(
        String name,
        int partitions,
        int replicationFactor,
        Map<String, String> configs
) {
}
