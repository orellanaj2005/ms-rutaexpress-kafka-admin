package cl.rutaexpress.kafkaadmin.exception;

/**
 * Resetear offsets o eliminar un grupo exige que no tenga consumidores conectados
 * (estado EMPTY). Si el microservicio sigue corriendo, se responde 409.
 */
public class GroupNotInactiveException extends RuntimeException {

    public GroupNotInactiveException(String message) {
        super(message);
    }
}
