package cl.rutaexpress.kafkaadmin.exception;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Traduce los errores del AdminClient y de validación a respuestas HTTP con un cuerpo
 * uniforme {"error": CODIGO, "message": texto}, igual que ms-rutaexpress-rabbitmq-admin:
 *
 * <ul>
 *   <li>400: validación, JSON mal formado y peticiones que Kafka rechaza por inválidas</li>
 *   <li>404: tópico o grupo inexistente</li>
 *   <li>409: ya existe, recurso protegido o grupo con consumidores activos</li>
 *   <li>503: Kafka no responde</li>
 * </ul>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    // ------------------------------------------------------------------ 400

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> fields = new LinkedHashMap<>();
        for (FieldError fieldError : ex.getBindingResult().getFieldErrors()) {
            fields.putIfAbsent(fieldError.getField(), fieldError.getDefaultMessage());
        }
        Map<String, Object> body = errorBody("VALIDATION_ERROR", "Uno o más campos no son válidos");
        body.put("fields", fields);
        return ResponseEntity.badRequest().body(body);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleUnreadable(HttpMessageNotReadableException ex) {
        return ResponseEntity.badRequest()
                .body(errorBody("MALFORMED_REQUEST", "El cuerpo de la petición no es un JSON válido o tiene valores no permitidos"));
    }

    @ExceptionHandler(InvalidOperationException.class)
    public ResponseEntity<Map<String, Object>> handleInvalidOperation(InvalidOperationException ex) {
        return ResponseEntity.badRequest().body(errorBody("INVALID_OPERATION", ex.getMessage()));
    }

    @ExceptionHandler({
            InvalidTopicException.class,
            InvalidPartitionsException.class,
            InvalidReplicationFactorException.class,
            InvalidConfigurationException.class,
            InvalidRequestException.class,
            PolicyViolationException.class
    })
    public ResponseEntity<Map<String, Object>> handleKafkaBadRequest(ApiException ex) {
        return ResponseEntity.badRequest().body(errorBody("KAFKA_REJECTED_REQUEST", ex.getMessage()));
    }

    // ------------------------------------------------------------------ 404

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNotFound(ResourceNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorBody("RESOURCE_NOT_FOUND", ex.getMessage()));
    }

    @ExceptionHandler(UnknownTopicOrPartitionException.class)
    public ResponseEntity<Map<String, Object>> handleUnknownTopic(UnknownTopicOrPartitionException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(errorBody("TOPIC_NOT_FOUND", "El tópico no existe"));
    }

    @ExceptionHandler(GroupIdNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleUnknownGroup(GroupIdNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(errorBody("GROUP_NOT_FOUND", "El consumer group no existe"));
    }

    // ------------------------------------------------------------------ 409

    @ExceptionHandler(TopicExistsException.class)
    public ResponseEntity<Map<String, Object>> handleTopicExists(TopicExistsException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(errorBody("TOPIC_ALREADY_EXISTS", ex.getMessage()));
    }

    @ExceptionHandler(ProtectedResourceException.class)
    public ResponseEntity<Map<String, Object>> handleProtected(ProtectedResourceException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(errorBody("PROTECTED_RESOURCE", ex.getMessage()));
    }

    @ExceptionHandler(GroupNotInactiveException.class)
    public ResponseEntity<Map<String, Object>> handleGroupActive(GroupNotInactiveException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(errorBody("GROUP_NOT_INACTIVE", ex.getMessage()));
    }

    /** Un consumidor se conectó al grupo justo entre la validación y el reseteo/eliminación. */
    @ExceptionHandler({GroupNotEmptyException.class, UnknownMemberIdException.class})
    public ResponseEntity<Map<String, Object>> handleGroupNotEmpty(ApiException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(errorBody("GROUP_NOT_INACTIVE", "El grupo tiene consumidores activos"));
    }

    // ------------------------------------------------------------------ 5xx

    @ExceptionHandler(KafkaUnavailableException.class)
    public ResponseEntity<Map<String, Object>> handleUnavailable(KafkaUnavailableException ex) {
        log.error("Kafka no disponible: {}", ex.getMessage(), ex.getCause());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(errorBody("KAFKA_UNAVAILABLE", ex.getMessage()));
    }

    /** Cualquier otro error que devuelva Kafka y no tenga una traducción propia. */
    @ExceptionHandler(ApiException.class)
    public ResponseEntity<Map<String, Object>> handleOtherKafkaError(ApiException ex) {
        log.error("Error de Kafka no clasificado: {}", ex.toString());
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(errorBody("KAFKA_ERROR", ex.getMessage()));
    }

    private Map<String, Object> errorBody(String error, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", error);
        body.put("message", message);
        return body;
    }
}
