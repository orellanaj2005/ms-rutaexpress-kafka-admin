package cl.rutaexpress.kafkaadmin.dto;

/** Hacia dónde se mueven los offsets de un consumer group en un reseteo. */
public enum OffsetResetStrategy {
    /** Primer mensaje que aún conserva el tópico: el grupo vuelve a procesar todo. */
    EARLIEST,
    /** Final del tópico: el grupo descarta lo pendiente y sigue solo con lo nuevo. */
    LATEST
}
