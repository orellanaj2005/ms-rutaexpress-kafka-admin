package cl.rutaexpress.kafkaadmin.dto;

/** Respuesta de POST /api/kafka/topics/{name}/partitions. */
public record PartitionsIncreasedResponse(
        String name,
        int previousPartitions,
        int partitions,
        String warning
) {
}
