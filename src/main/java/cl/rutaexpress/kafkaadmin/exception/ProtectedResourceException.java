package cl.rutaexpress.kafkaadmin.exception;

/** Se intentó eliminar un tópico o grupo base del sistema (409). */
public class ProtectedResourceException extends RuntimeException {

    public ProtectedResourceException(String message) {
        super(message);
    }
}
