package cl.rutaexpress.kafkaadmin.dto;

/**
 * Fila de la lista de consumer groups (GET /api/kafka/consumer-groups).
 *
 * @param state STABLE (consumiendo), EMPTY (sin consumidores activos), PREPARING_REBALANCE,
 *              COMPLETING_REBALANCE, DEAD o UNKNOWN
 */
public record ConsumerGroupSummary(
        String groupId,
        String state,
        boolean simpleConsumerGroup,
        boolean protectedGroup
) {
}
