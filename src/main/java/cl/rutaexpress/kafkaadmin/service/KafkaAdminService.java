package cl.rutaexpress.kafkaadmin.service;

import cl.rutaexpress.kafkaadmin.config.KafkaAdminProperties;
import cl.rutaexpress.kafkaadmin.dto.ConsumerGroupDetail;
import cl.rutaexpress.kafkaadmin.dto.ConsumerGroupLag;
import cl.rutaexpress.kafkaadmin.dto.ConsumerGroupSummary;
import cl.rutaexpress.kafkaadmin.dto.CreateTopicRequest;
import cl.rutaexpress.kafkaadmin.dto.GroupMember;
import cl.rutaexpress.kafkaadmin.dto.IncreasePartitionsRequest;
import cl.rutaexpress.kafkaadmin.dto.OffsetResetStrategy;
import cl.rutaexpress.kafkaadmin.dto.PartitionDetail;
import cl.rutaexpress.kafkaadmin.dto.PartitionLag;
import cl.rutaexpress.kafkaadmin.dto.PartitionOffsetChange;
import cl.rutaexpress.kafkaadmin.dto.PartitionsIncreasedResponse;
import cl.rutaexpress.kafkaadmin.dto.ResetOffsetsRequest;
import cl.rutaexpress.kafkaadmin.dto.ResetOffsetsResponse;
import cl.rutaexpress.kafkaadmin.dto.TopicConfigEntry;
import cl.rutaexpress.kafkaadmin.dto.TopicCreatedResponse;
import cl.rutaexpress.kafkaadmin.dto.TopicDetail;
import cl.rutaexpress.kafkaadmin.dto.TopicPartitionRef;
import cl.rutaexpress.kafkaadmin.dto.TopicSummary;
import cl.rutaexpress.kafkaadmin.dto.UpdateTopicConfigsRequest;
import cl.rutaexpress.kafkaadmin.exception.GroupNotInactiveException;
import cl.rutaexpress.kafkaadmin.exception.InvalidOperationException;
import cl.rutaexpress.kafkaadmin.exception.KafkaUnavailableException;
import cl.rutaexpress.kafkaadmin.exception.ProtectedResourceException;
import cl.rutaexpress.kafkaadmin.exception.ResourceNotFoundException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Capa de gestión sobre el AdminClient de Kafka. El controller solo delega aquí;
 * este servicio valida las reglas de negocio (tópicos protegidos, solo aumentar
 * particiones, grupo inactivo para resetear...) y transforma las respuestas de Kafka
 * a DTOs propios, para no exponer las clases internas del cliente.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    private static final String RETENTION_MS = "retention.ms";
    private static final String CLEANUP_POLICY = "cleanup.policy";
    private static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";
    private static final String INTERNAL_PREFIX = "__";

    private static final Comparator<TopicPartition> BY_TOPIC_AND_PARTITION =
            Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    private final AdminClient admin;
    private final KafkaAdminProperties properties;

    public KafkaAdminService(AdminClient admin, KafkaAdminProperties properties) {
        this.admin = admin;
        this.properties = properties;
    }

    // ================================================================ tópicos

    public List<TopicSummary> listTopics(boolean includeInternal) {
        Collection<TopicListing> listings = await(
                admin.listTopics(new ListTopicsOptions().listInternal(includeInternal)).listings(),
                "listar los tópicos");

        List<String> names = listings.stream().map(TopicListing::name).sorted().toList();
        if (names.isEmpty()) {
            return List.of();
        }

        Map<String, TopicDescription> descriptions = await(
                admin.describeTopics(names).allTopicNames(), "describir los tópicos");

        List<ConfigResource> resources = names.stream().map(KafkaAdminService::topicResource).toList();
        Map<ConfigResource, Config> configs = await(
                admin.describeConfigs(resources).all(), "leer la configuración de los tópicos");

        List<TopicSummary> result = new ArrayList<>();
        for (String name : names) {
            TopicDescription description = descriptions.get(name);
            if (description == null) {
                continue; // eliminado entre el listado y la descripción
            }
            Config config = configs.get(topicResource(name));
            result.add(new TopicSummary(
                    name,
                    description.partitions().size(),
                    replicationFactor(description),
                    parseLong(configValue(config, RETENTION_MS)),
                    configValue(config, CLEANUP_POLICY),
                    parseInteger(configValue(config, MIN_INSYNC_REPLICAS)),
                    description.isInternal(),
                    properties.isProtectedTopic(name)));
        }
        return result;
    }

    public TopicDetail describeTopic(String name) {
        TopicDescription description = describeTopicOrThrow(name);
        List<PartitionDetail> partitions = description.partitions().stream()
                .sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
                .map(p -> new PartitionDetail(
                        p.partition(),
                        nodeId(p.leader()),
                        p.replicas().stream().map(Node::id).toList(),
                        p.isr().stream().map(Node::id).toList()))
                .toList();

        return new TopicDetail(
                description.name(),
                description.topicId() == null ? null : description.topicId().toString(),
                description.isInternal(),
                properties.isProtectedTopic(name),
                partitions.size(),
                replicationFactor(description),
                partitions);
    }

    public TopicCreatedResponse createTopic(CreateTopicRequest request) {
        String name = request.name();
        if (name.startsWith(INTERNAL_PREFIX)) {
            throw new InvalidOperationException("El prefijo '__' está reservado para los tópicos internos de Kafka");
        }

        Map<String, String> configs = new LinkedHashMap<>(request.configs());
        rejectNonEditableConfigs(configs.keySet());
        int replicationFactor = request.replicationFactor();
        validateMinInsyncReplicas(configs.get(MIN_INSYNC_REPLICAS), replicationFactor);

        // Los brokers tienen min.insync.replicas=2: un tópico con 1 réplica quedaría sin poder
        // recibir mensajes con acks=all. En ese caso se ajusta al único broker disponible.
        if (replicationFactor < 2 && !configs.containsKey(MIN_INSYNC_REPLICAS)) {
            configs.put(MIN_INSYNC_REPLICAS, "1");
        }

        NewTopic newTopic = new NewTopic(name, request.partitions(), (short) replicationFactor).configs(configs);
        try {
            await(admin.createTopics(List.of(newTopic)).all(), "crear el tópico " + name);
        } catch (TopicExistsException ex) {
            throw new TopicExistsException("Ya existe un tópico llamado '" + name + "'");
        }

        log.info("Tópico creado: name={} partitions={} replicationFactor={} configs={} user={}",
                name, request.partitions(), replicationFactor, configs, currentUser());
        return new TopicCreatedResponse(name, request.partitions(), replicationFactor, Map.copyOf(configs));
    }

    public void deleteTopic(String name) {
        if (properties.isProtectedTopic(name)) {
            throw new ProtectedResourceException(
                    "El tópico '" + name + "' es parte del contrato de eventos del sistema y no se puede eliminar");
        }
        try {
            await(admin.deleteTopics(List.of(name)).all(), "eliminar el tópico " + name);
        } catch (UnknownTopicOrPartitionException ex) {
            throw topicNotFound(name);
        }
        log.info("Tópico eliminado: name={} user={}", name, currentUser());
    }

    public List<TopicConfigEntry> getTopicConfigs(String name, boolean onlyOverrides) {
        ConfigResource resource = topicResource(name);
        Config config;
        try {
            config = await(admin.describeConfigs(List.of(resource)).all(),
                    "leer la configuración del tópico " + name).get(resource);
        } catch (UnknownTopicOrPartitionException ex) {
            throw topicNotFound(name);
        }
        if (config == null) {
            throw topicNotFound(name);
        }

        return config.entries().stream()
                .filter(e -> !onlyOverrides || e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                .sorted(Comparator.comparing(ConfigEntry::name))
                .map(e -> new TopicConfigEntry(
                        e.name(),
                        e.isSensitive() ? null : e.value(),
                        e.source() == null ? null : e.source().name(),
                        e.isDefault(),
                        e.isReadOnly(),
                        e.isSensitive()))
                .toList();
    }

    public void updateTopicConfigs(String name, UpdateTopicConfigsRequest request) {
        if (request.isEmpty()) {
            throw new InvalidOperationException("Indica al menos una configuración en 'set' o en 'delete'");
        }
        rejectNonEditableConfigs(request.set().keySet());
        rejectNonEditableConfigs(request.delete());

        Set<String> both = new HashSet<>(request.set().keySet());
        both.retainAll(request.delete());
        if (!both.isEmpty()) {
            throw new InvalidOperationException("Una configuración no puede estar en 'set' y en 'delete' a la vez: " + both);
        }
        for (Map.Entry<String, String> entry : request.set().entrySet()) {
            if (entry.getValue() == null || entry.getValue().isBlank()) {
                throw new InvalidOperationException("La configuración '" + entry.getKey() + "' necesita un valor");
            }
        }

        if (request.set().containsKey(MIN_INSYNC_REPLICAS)) {
            int replicationFactor = replicationFactor(describeTopicOrThrow(name));
            validateMinInsyncReplicas(request.set().get(MIN_INSYNC_REPLICAS), replicationFactor);
        }

        List<AlterConfigOp> ops = new ArrayList<>();
        request.set().forEach((key, value) ->
                ops.add(new AlterConfigOp(new ConfigEntry(key, value.trim()), AlterConfigOp.OpType.SET)));
        request.delete().forEach(key ->
                ops.add(new AlterConfigOp(new ConfigEntry(key, ""), AlterConfigOp.OpType.DELETE)));

        try {
            await(admin.incrementalAlterConfigs(Map.of(topicResource(name), ops)).all(),
                    "modificar la configuración del tópico " + name);
        } catch (UnknownTopicOrPartitionException ex) {
            throw topicNotFound(name);
        }
        log.info("Configuración modificada: topic={} set={} delete={} user={}",
                name, request.set(), request.delete(), currentUser());
    }

    public PartitionsIncreasedResponse increasePartitions(String name, IncreasePartitionsRequest request) {
        int current = describeTopicOrThrow(name).partitions().size();
        int total = request.totalPartitions();
        if (total <= current) {
            throw new InvalidOperationException("El tópico '" + name + "' ya tiene " + current
                    + " particiones. Kafka solo permite aumentarlas: indica un total mayor a " + current);
        }

        try {
            await(admin.createPartitions(Map.of(name, NewPartitions.increaseTo(total))).all(),
                    "aumentar las particiones del tópico " + name);
        } catch (UnknownTopicOrPartitionException ex) {
            throw topicNotFound(name);
        }
        log.info("Particiones aumentadas: topic={} {} -> {} user={}", name, current, total, currentUser());

        return new PartitionsIncreasedResponse(name, current, total,
                "Con más particiones cambia la partición que corresponde a cada clave: el orden por envío "
                        + "se garantiza para los mensajes nuevos, no entre mensajes anteriores y posteriores al cambio.");
    }

    // ======================================================= consumer groups

    public List<ConsumerGroupSummary> listConsumerGroups() {
        Collection<ConsumerGroupListing> groups = await(admin.listConsumerGroups().all(), "listar los consumer groups");
        return groups.stream()
                .sorted(Comparator.comparing(ConsumerGroupListing::groupId))
                .map(g -> new ConsumerGroupSummary(
                        g.groupId(),
                        g.state().map(Enum::name).orElse(ConsumerGroupState.UNKNOWN.name()),
                        g.isSimpleConsumerGroup(),
                        properties.isProtectedGroup(g.groupId())))
                .toList();
    }

    public ConsumerGroupDetail describeConsumerGroup(String groupId) {
        ConsumerGroupDescription description = describeGroupOrThrow(groupId);

        List<GroupMember> members = description.members().stream()
                .sorted(Comparator.comparing(MemberDescription::clientId, Comparator.nullsLast(Comparator.naturalOrder()))
                        .thenComparing(MemberDescription::consumerId))
                .map(m -> new GroupMember(
                        m.consumerId(),
                        m.clientId(),
                        m.host(),
                        m.assignment().topicPartitions().stream()
                                .sorted(BY_TOPIC_AND_PARTITION)
                                .map(tp -> new TopicPartitionRef(tp.topic(), tp.partition()))
                                .toList()))
                .toList();

        return new ConsumerGroupDetail(
                description.groupId(),
                description.state().name(),
                emptyToNull(description.partitionAssignor()),
                nodeId(description.coordinator()),
                properties.isProtectedGroup(groupId),
                members);
    }

    /**
     * Lag real por partición: offset final del tópico (listOffsets latest) menos el último
     * offset confirmado por el grupo. Incluye también las particiones asignadas que aún no
     * tienen offset confirmado (lag null).
     */
    public ConsumerGroupLag getConsumerGroupLag(String groupId) {
        ConsumerGroupDescription description = describeGroupOrThrow(groupId);
        Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets(groupId);

        Map<TopicPartition, MemberDescription> owners = new HashMap<>();
        for (MemberDescription member : description.members()) {
            for (TopicPartition tp : member.assignment().topicPartitions()) {
                owners.put(tp, member);
            }
        }

        Set<TopicPartition> partitions = new HashSet<>(committed.keySet());
        partitions.addAll(owners.keySet());
        if (partitions.isEmpty()) {
            return new ConsumerGroupLag(groupId, description.state().name(), 0L, List.of());
        }

        Map<TopicPartition, ListOffsetsResultInfo> endOffsets =
                listOffsets(partitions, OffsetSpec.latest(), "leer el final de las particiones del grupo " + groupId);

        long totalLag = 0;
        List<PartitionLag> rows = new ArrayList<>();
        for (TopicPartition tp : partitions.stream().sorted(BY_TOPIC_AND_PARTITION).toList()) {
            ListOffsetsResultInfo end = endOffsets.get(tp);
            if (end == null) {
                continue;
            }
            OffsetAndMetadata commit = committed.get(tp);
            Long committedOffset = commit == null ? null : commit.offset();
            Long lag = committedOffset == null ? null : Math.max(0L, end.offset() - committedOffset);
            if (lag != null) {
                totalLag += lag;
            }
            MemberDescription owner = owners.get(tp);
            rows.add(new PartitionLag(
                    tp.topic(), tp.partition(), committedOffset, end.offset(), lag,
                    owner == null ? null : owner.consumerId(),
                    owner == null ? null : owner.clientId(),
                    owner == null ? null : owner.host()));
        }
        return new ConsumerGroupLag(groupId, description.state().name(), totalLag, rows);
    }

    /**
     * Mueve los offsets del grupo al inicio (EARLIEST) o al final (LATEST). Kafka solo lo
     * permite sin consumidores conectados, por eso se exige estado EMPTY: primero hay que
     * detener el microservicio dueño del grupo.
     */
    public ResetOffsetsResponse resetOffsets(String groupId, ResetOffsetsRequest request) {
        ConsumerGroupDescription description = describeGroupOrThrow(groupId);
        requireInactive(description, "resetear sus offsets");

        Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets(groupId);
        Set<TopicPartition> targets;
        if (request.topic() != null) {
            targets = committed.keySet().stream()
                    .filter(tp -> tp.topic().equals(request.topic()))
                    .collect(Collectors.toCollection(HashSet::new));
            if (targets.isEmpty()) {
                // El grupo aún no consume ese tópico: se resetean todas sus particiones.
                targets = describeTopicOrThrow(request.topic()).partitions().stream()
                        .map(p -> new TopicPartition(request.topic(), p.partition()))
                        .collect(Collectors.toCollection(HashSet::new));
            }
        } else {
            targets = new HashSet<>(committed.keySet());
            if (targets.isEmpty()) {
                throw new InvalidOperationException("El grupo '" + groupId
                        + "' no tiene offsets confirmados; indica en 'topic' qué tópico resetear");
            }
        }

        OffsetSpec spec = request.strategy() == OffsetResetStrategy.EARLIEST ? OffsetSpec.earliest() : OffsetSpec.latest();
        Map<TopicPartition, ListOffsetsResultInfo> newOffsets =
                listOffsets(targets, spec, "calcular los nuevos offsets del grupo " + groupId);

        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        List<PartitionOffsetChange> changes = new ArrayList<>();
        for (TopicPartition tp : targets.stream().sorted(BY_TOPIC_AND_PARTITION).toList()) {
            ListOffsetsResultInfo info = newOffsets.get(tp);
            if (info == null) {
                continue;
            }
            toCommit.put(tp, new OffsetAndMetadata(info.offset()));
            OffsetAndMetadata previous = committed.get(tp);
            changes.add(new PartitionOffsetChange(tp.topic(), tp.partition(),
                    previous == null ? null : previous.offset(), info.offset()));
        }

        await(admin.alterConsumerGroupOffsets(groupId, toCommit).all(), "resetear los offsets del grupo " + groupId);
        log.info("Offsets reseteados: group={} strategy={} topic={} partitions={} user={}",
                groupId, request.strategy(), request.topic(), toCommit.size(), currentUser());
        return new ResetOffsetsResponse(groupId, request.strategy(), changes);
    }

    public void deleteConsumerGroup(String groupId) {
        if (properties.isProtectedGroup(groupId)) {
            throw new ProtectedResourceException("El grupo '" + groupId
                    + "' pertenece a un microservicio del sistema y no se puede eliminar (sí se puede resetear)");
        }
        ConsumerGroupDescription description = describeGroupOrThrow(groupId);
        requireInactive(description, "eliminarlo");

        try {
            await(admin.deleteConsumerGroups(List.of(groupId)).all(), "eliminar el grupo " + groupId);
        } catch (GroupIdNotFoundException ex) {
            throw groupNotFound(groupId);
        }
        log.info("Consumer group eliminado: group={} user={}", groupId, currentUser());
    }

    // =============================================================== helpers

    private TopicDescription describeTopicOrThrow(String name) {
        KafkaFuture<TopicDescription> future = admin.describeTopics(List.of(name)).topicNameValues().get(name);
        if (future == null) {
            throw topicNotFound(name);
        }
        try {
            return await(future, "describir el tópico " + name);
        } catch (UnknownTopicOrPartitionException ex) {
            throw topicNotFound(name);
        }
    }

    private ConsumerGroupDescription describeGroupOrThrow(String groupId) {
        KafkaFuture<ConsumerGroupDescription> future =
                admin.describeConsumerGroups(List.of(groupId)).describedGroups().get(groupId);
        if (future == null) {
            throw groupNotFound(groupId);
        }
        ConsumerGroupDescription description;
        try {
            description = await(future, "describir el grupo " + groupId);
        } catch (GroupIdNotFoundException ex) {
            throw groupNotFound(groupId);
        }
        // Kafka responde DEAD (sin error) cuando el grupo no existe.
        if (description.state() == ConsumerGroupState.DEAD) {
            throw groupNotFound(groupId);
        }
        return description;
    }

    private Map<TopicPartition, OffsetAndMetadata> committedOffsets(String groupId) {
        Map<TopicPartition, OffsetAndMetadata> raw = await(
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
                "leer los offsets del grupo " + groupId);
        Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
        raw.forEach((tp, offset) -> {
            if (offset != null) {
                committed.put(tp, offset);
            }
        });
        return committed;
    }

    private Map<TopicPartition, ListOffsetsResultInfo> listOffsets(Collection<TopicPartition> partitions,
                                                                   OffsetSpec spec, String action) {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        partitions.forEach(tp -> request.put(tp, spec));
        return await(admin.listOffsets(request).all(), action);
    }

    private void requireInactive(ConsumerGroupDescription description, String action) {
        if (description.state() != ConsumerGroupState.EMPTY) {
            throw new GroupNotInactiveException("El grupo '" + description.groupId() + "' está en estado "
                    + description.state() + " con " + description.members().size()
                    + " consumidor(es) conectado(s). Detén el microservicio antes de " + action);
        }
    }

    private void rejectNonEditableConfigs(Collection<String> keys) {
        List<String> invalid = keys.stream().filter(k -> !properties.isEditableConfig(k)).sorted().toList();
        if (!invalid.isEmpty()) {
            throw new InvalidOperationException("Configuraciones no permitidas: " + invalid
                    + ". Se pueden modificar: " + properties.editableConfigs());
        }
    }

    private static void validateMinInsyncReplicas(String value, int replicationFactor) {
        if (value == null) {
            return;
        }
        Integer minIsr = parseInteger(value.trim());
        if (minIsr == null || minIsr < 1 || minIsr > replicationFactor) {
            throw new InvalidOperationException("min.insync.replicas debe ser un número entre 1 y el factor de replicación ("
                    + replicationFactor + ")");
        }
    }

    /**
     * Espera el resultado del AdminClient. Las excepciones de Kafka (ApiException) se
     * relanzan tal cual para que GlobalExceptionHandler las traduzca a 400/404/409.
     */
    private <T> T await(KafkaFuture<T> future, String action) {
        try {
            return future.get(properties.operationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaUnavailableException("Se interrumpió la operación al " + action, ex);
        } catch (TimeoutException ex) {
            throw new KafkaUnavailableException("Kafka no respondió a tiempo al " + action, ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof org.apache.kafka.common.errors.TimeoutException) {
                throw new KafkaUnavailableException("Kafka no respondió a tiempo al " + action, cause);
            }
            if (cause instanceof ApiException apiException) {
                throw apiException;
            }
            throw new KafkaUnavailableException("Error inesperado al " + action, cause);
        }
    }

    private static ConfigResource topicResource(String name) {
        return new ConfigResource(ConfigResource.Type.TOPIC, name);
    }

    private static int replicationFactor(TopicDescription description) {
        return description.partitions().isEmpty() ? 0 : description.partitions().get(0).replicas().size();
    }

    private static String configValue(Config config, String key) {
        if (config == null) {
            return null;
        }
        ConfigEntry entry = config.get(key);
        return entry == null ? null : entry.value();
    }

    private static Long parseLong(String value) {
        try {
            return value == null ? null : Long.parseLong(value);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static Integer parseInteger(String value) {
        try {
            return value == null ? null : Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static Integer nodeId(Node node) {
        return node == null || node.isEmpty() ? null : node.id();
    }

    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }

    private static ResourceNotFoundException topicNotFound(String name) {
        return new ResourceNotFoundException("No existe el tópico '" + name + "'");
    }

    private static ResourceNotFoundException groupNotFound(String groupId) {
        return new ResourceNotFoundException("No existe el consumer group '" + groupId + "'");
    }

    /** Usuario del JWT, solo para dejar registro en los logs de quién hizo cada cambio. */
    private static String currentUser() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof JwtAuthenticationToken jwt) {
            Object username = jwt.getToken().getClaims().get("preferred_username");
            return Objects.toString(username, jwt.getName());
        }
        return auth == null ? "desconocido" : auth.getName();
    }
}
