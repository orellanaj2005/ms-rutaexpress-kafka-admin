package cl.rutaexpress.kafkaadmin.dto;

public record TopicPartitionRef(
        String topic,
        int partition
) {
}
