package cl.rutaexpress.kafkaadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.List;

/**
 * Reglas de administración leídas desde application.yaml (prefijo rutaexpress.kafka).
 *
 * @param protectedTopics         tópicos del contrato (tabla 3.3) que no se pueden eliminar
 * @param protectedGroups         consumer groups de los microservicios que no se pueden eliminar
 *                                (sí se pueden resetear, con el grupo inactivo)
 * @param editableConfigs         configuraciones de tópico que se aceptan en PUT /configs
 * @param operationTimeoutSeconds tiempo máximo de espera de cada operación del AdminClient
 */
@ConfigurationProperties(prefix = "rutaexpress.kafka")
public record KafkaAdminProperties(
        List<String> protectedTopics,
        List<String> protectedGroups,
        List<String> editableConfigs,
        @DefaultValue("15") long operationTimeoutSeconds
) {

    /** Los nombres que empiezan con "__" son internos de Kafka (__consumer_offsets, etc.). */
    private static final String INTERNAL_PREFIX = "__";

    public KafkaAdminProperties {
        protectedTopics = protectedTopics == null ? List.of() : List.copyOf(protectedTopics);
        protectedGroups = protectedGroups == null ? List.of() : List.copyOf(protectedGroups);
        editableConfigs = editableConfigs == null ? List.of() : List.copyOf(editableConfigs);
    }

    public boolean isProtectedTopic(String name) {
        return name != null && (name.startsWith(INTERNAL_PREFIX) || protectedTopics.contains(name));
    }

    public boolean isProtectedGroup(String groupId) {
        return groupId != null && protectedGroups.contains(groupId);
    }

    public boolean isEditableConfig(String key) {
        return key != null && editableConfigs.contains(key);
    }
}
