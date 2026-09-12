package cl.rutaexpress.kafkaadmin.controller;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.*;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final AdminClient adminClient;

    public KafkaAdminController(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @GetMapping("/topics")
    public ResponseEntity<Object> listTopics() throws ExecutionException, InterruptedException {
        Set<String> topics = adminClient.listTopics().names().get();
        return ResponseEntity.ok(topics);
    }

    @GetMapping("/topics/{name}")
    public ResponseEntity<Object> describeTopic(@PathVariable String name) throws ExecutionException, InterruptedException {
        TopicDescription description = adminClient.describeTopics(List.of(name))
            .topicNameValues().get(name).get();
        return ResponseEntity.ok(description);
    }

    @GetMapping("/consumer-groups")
    public ResponseEntity<Object> listConsumerGroups() throws ExecutionException, InterruptedException {
        Collection<ConsumerGroupListing> groups = adminClient.listConsumerGroups().all().get();
        return ResponseEntity.ok(groups);
    }

    @GetMapping("/consumer-groups/{groupId}/lag")
    public ResponseEntity<Object> getConsumerGroupLag(@PathVariable String groupId) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> offsets =
            adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
        return ResponseEntity.ok(offsets);
    }
}