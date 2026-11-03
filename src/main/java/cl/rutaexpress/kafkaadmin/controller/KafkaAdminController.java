package cl.rutaexpress.kafkaadmin.controller;

import cl.rutaexpress.kafkaadmin.dto.ConsumerGroupDetail;
import cl.rutaexpress.kafkaadmin.dto.ConsumerGroupLag;
import cl.rutaexpress.kafkaadmin.dto.ConsumerGroupSummary;
import cl.rutaexpress.kafkaadmin.dto.CreateTopicRequest;
import cl.rutaexpress.kafkaadmin.dto.IncreasePartitionsRequest;
import cl.rutaexpress.kafkaadmin.dto.PartitionsIncreasedResponse;
import cl.rutaexpress.kafkaadmin.dto.ResetOffsetsRequest;
import cl.rutaexpress.kafkaadmin.dto.ResetOffsetsResponse;
import cl.rutaexpress.kafkaadmin.dto.TopicConfigEntry;
import cl.rutaexpress.kafkaadmin.dto.TopicCreatedResponse;
import cl.rutaexpress.kafkaadmin.dto.TopicDetail;
import cl.rutaexpress.kafkaadmin.dto.TopicSummary;
import cl.rutaexpress.kafkaadmin.dto.UpdateTopicConfigsRequest;
import cl.rutaexpress.kafkaadmin.service.KafkaAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * API de administración de Kafka. Toda la lógica está en KafkaAdminService; aquí solo
 * se valida la entrada y se elige el código HTTP. Los errores se traducen en
 * GlobalExceptionHandler.
 */
@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    // ---------------------------------------------------------------- tópicos

    @GetMapping("/topics")
    public List<TopicSummary> listTopics(@RequestParam(defaultValue = "false") boolean includeInternal) {
        return service.listTopics(includeInternal);
    }

    @GetMapping("/topics/{name}")
    public TopicDetail describeTopic(@PathVariable String name) {
        return service.describeTopic(name);
    }

    @PostMapping("/topics")
    public ResponseEntity<TopicCreatedResponse> createTopic(@Valid @RequestBody CreateTopicRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(service.createTopic(request));
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Void> deleteTopic(@PathVariable String name) {
        service.deleteTopic(name);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/topics/{name}/configs")
    public List<TopicConfigEntry> getTopicConfigs(@PathVariable String name,
                                                  @RequestParam(defaultValue = "false") boolean onlyOverrides) {
        return service.getTopicConfigs(name, onlyOverrides);
    }

    @PutMapping("/topics/{name}/configs")
    public ResponseEntity<Void> updateTopicConfigs(@PathVariable String name,
                                                   @Valid @RequestBody UpdateTopicConfigsRequest request) {
        service.updateTopicConfigs(name, request);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/topics/{name}/partitions")
    public PartitionsIncreasedResponse increasePartitions(@PathVariable String name,
                                                          @Valid @RequestBody IncreasePartitionsRequest request) {
        return service.increasePartitions(name, request);
    }

    // -------------------------------------------------------- consumer groups

    @GetMapping("/consumer-groups")
    public List<ConsumerGroupSummary> listConsumerGroups() {
        return service.listConsumerGroups();
    }

    @GetMapping("/consumer-groups/{groupId}")
    public ConsumerGroupDetail describeConsumerGroup(@PathVariable String groupId) {
        return service.describeConsumerGroup(groupId);
    }

    @GetMapping("/consumer-groups/{groupId}/lag")
    public ConsumerGroupLag getConsumerGroupLag(@PathVariable String groupId) {
        return service.getConsumerGroupLag(groupId);
    }

    @PostMapping("/consumer-groups/{groupId}/reset")
    public ResetOffsetsResponse resetOffsets(@PathVariable String groupId,
                                             @Valid @RequestBody ResetOffsetsRequest request) {
        return service.resetOffsets(groupId, request);
    }

    @DeleteMapping("/consumer-groups/{groupId}")
    public ResponseEntity<Void> deleteConsumerGroup(@PathVariable String groupId) {
        service.deleteConsumerGroup(groupId);
        return ResponseEntity.noContent().build();
    }
}
