package cl.rutaexpress.kafkaadmin.exception;

/**
 * La petición está bien formada pero no tiene sentido para Kafka: reducir particiones,
 * una configuración no editable, min.insync.replicas mayor que el factor de replicación... (400).
 */
public class InvalidOperationException extends RuntimeException {

    public InvalidOperationException(String message) {
        super(message);
    }
}
