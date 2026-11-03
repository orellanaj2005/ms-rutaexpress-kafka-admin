package cl.rutaexpress.kafkaadmin.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

/**
 * Cuerpo de POST /api/kafka/consumer-groups/{id}/reset.
 *
 * <pre>
 * { "strategy": "EARLIEST", "topic": "shipments.events" }
 * </pre>
 *
 * @param strategy EARLIEST o LATEST
 * @param topic    opcional: limita el reseteo a un tópico. Sin él se resetean todas las
 *                 particiones en que el grupo tiene offsets confirmados.
 */
public record ResetOffsetsRequest(

        @NotNull(message = "La estrategia es obligatoria (EARLIEST o LATEST)")
        OffsetResetStrategy strategy,

        @Pattern(regexp = "^[a-zA-Z0-9._-]{1,249}$", message = "El nombre del tópico no es válido")
        String topic
) {
    public ResetOffsetsRequest {
        if (topic != null) {
            topic = topic.trim();
            if (topic.isEmpty()) {
                topic = null;
            }
        }
    }
}
