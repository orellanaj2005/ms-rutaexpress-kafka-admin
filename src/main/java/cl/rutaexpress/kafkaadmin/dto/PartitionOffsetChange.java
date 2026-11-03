package cl.rutaexpress.kafkaadmin.dto;

/** Offset de una partición antes y después de un reseteo. */
public record PartitionOffsetChange(
        String topic,
        int partition,
        Long previousOffset,
        long newOffset
) {
}
