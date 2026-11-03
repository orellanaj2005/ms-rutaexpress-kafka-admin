package cl.rutaexpress.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Cuerpo de POST /api/kafka/topics/{name}/partitions. Indica el total de particiones
 * que debe quedar (no cuántas agregar). Kafka solo permite aumentar.
 */
public record IncreasePartitionsRequest(

        @NotNull(message = "El total de particiones es obligatorio")
        @Min(value = 1, message = "El total de particiones debe ser al menos 1")
        @Max(value = 100, message = "El tópico no puede tener más de 100 particiones")
        Integer totalPartitions
) {
}
