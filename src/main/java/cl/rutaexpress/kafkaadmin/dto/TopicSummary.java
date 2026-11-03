package cl.rutaexpress.kafkaadmin.dto;

/**
 * Fila de la tabla de tópicos (GET /api/kafka/topics).
 *
 * @param retentionMs    retention.ms efectivo; -1 significa sin límite de tiempo
 * @param protectedTopic true si es un tópico base del sistema y no se puede eliminar
 */
public record TopicSummary(
        String name,
        int partitions,
        int replicationFactor,
        Long retentionMs,
        String cleanupPolicy,
        Integer minInsyncReplicas,
        boolean internal,
        boolean protectedTopic
) {
}
