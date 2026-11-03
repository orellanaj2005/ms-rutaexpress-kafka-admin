package cl.rutaexpress.kafkaadmin.dto;

/**
 * Una configuración efectiva de un tópico (GET /api/kafka/topics/{name}/configs).
 *
 * @param source origen del valor: DYNAMIC_TOPIC_CONFIG (fijado para este tópico),
 *               STATIC_BROKER_CONFIG (del compose del broker), DEFAULT_CONFIG (por defecto de Kafka)...
 * @param value  null si la configuración es sensible
 */
public record TopicConfigEntry(
        String name,
        String value,
        String source,
        boolean isDefault,
        boolean readOnly,
        boolean sensitive
) {
}
