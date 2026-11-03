package cl.rutaexpress.kafkaadmin.exception;

/** Kafka no respondió dentro del tiempo de espera o la operación fue interrumpida (503). */
public class KafkaUnavailableException extends RuntimeException {

    public KafkaUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
