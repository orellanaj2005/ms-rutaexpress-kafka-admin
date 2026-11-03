package cl.rutaexpress.kafkaadmin.dto;

/**
 * Lag de una partición: endOffset (último offset del tópico, listOffsets) menos
 * committedOffset (último offset confirmado por el grupo).
 *
 * @param committedOffset null si el grupo tiene la partición asignada pero aún no confirma nada
 * @param lag             mensajes pendientes; null si no hay offset confirmado
 * @param clientId        consumidor que tiene asignada la partición; null si el grupo está inactivo
 */
public record PartitionLag(
        String topic,
        int partition,
        Long committedOffset,
        long endOffset,
        Long lag,
        String memberId,
        String clientId,
        String host
) {
}
