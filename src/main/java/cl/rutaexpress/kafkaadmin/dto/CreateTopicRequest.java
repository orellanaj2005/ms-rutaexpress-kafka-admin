package cl.rutaexpress.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.util.Map;

/**
 * Cuerpo de POST /api/kafka/topics.
 *
 * <pre>
 * {
 *   "name": "pruebas.demo",
 *   "partitions": 3,
 *   "replicationFactor": 3,
 *   "configs": { "retention.ms": "86400000", "cleanup.policy": "delete" }
 * }
 * </pre>
 */
public record CreateTopicRequest(

        @NotBlank(message = "El nombre del tópico es obligatorio")
        @Pattern(
                regexp = "^[a-zA-Z0-9._-]{1,249}$",
                message = "El nombre del tópico solo puede contener letras, números, puntos, guiones y guiones bajos (máximo 249 caracteres)"
        )
        String name,

        @NotNull(message = "La cantidad de particiones es obligatoria")
        @Min(value = 1, message = "El tópico debe tener al menos 1 partición")
        @Max(value = 100, message = "El tópico no puede tener más de 100 particiones")
        Integer partitions,

        @NotNull(message = "El factor de replicación es obligatorio")
        @Min(value = 1, message = "El factor de replicación debe ser al menos 1")
        @Max(value = 3, message = "El factor de replicación no puede superar 3 (el clúster tiene 3 brokers)")
        Integer replicationFactor,

        Map<String, String> configs
) {
    public CreateTopicRequest {
        if (name != null) {
            name = name.trim();
        }
        configs = configs == null ? Map.of() : Map.copyOf(configs);
    }
}
