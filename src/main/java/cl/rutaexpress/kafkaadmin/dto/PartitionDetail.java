package cl.rutaexpress.kafkaadmin.dto;

import java.util.List;

/**
 * Estado de una partición: broker líder, brokers con réplica y réplicas sincronizadas (ISR).
 * Si isr tiene menos elementos que replicas, algún broker está atrasado o caído.
 */
public record PartitionDetail(
        int partition,
        Integer leader,
        List<Integer> replicas,
        List<Integer> isr
) {
}
