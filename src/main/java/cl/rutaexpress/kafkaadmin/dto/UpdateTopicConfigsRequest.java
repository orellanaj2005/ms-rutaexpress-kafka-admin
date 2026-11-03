package cl.rutaexpress.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

/**
 * Cuerpo de PUT /api/kafka/topics/{name}/configs. Se aplica con incrementalAlterConfigs,
 * por lo que solo cambia lo indicado y el resto de la configuración queda igual.
 *
 * <pre>
 * {
 *   "set":    { "retention.ms": "259200000", "cleanup.policy": "compact,delete" },
 *   "delete": [ "segment.ms" ]
 * }
 * </pre>
 *
 * @param set    configuraciones que se fijan con un valor
 * @param delete configuraciones que vuelven al valor por defecto del broker
 */
public record UpdateTopicConfigsRequest(
        Map<String, String> set,
        List<String> delete
) {
    public UpdateTopicConfigsRequest {
        set = set == null ? Map.of() : Map.copyOf(set);
        delete = delete == null ? List.of() : List.copyOf(delete);
    }

    public boolean isEmpty() {
        return set.isEmpty() && delete.isEmpty();
    }
}
