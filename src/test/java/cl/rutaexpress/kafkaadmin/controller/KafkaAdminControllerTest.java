package cl.rutaexpress.kafkaadmin.controller;

import cl.rutaexpress.kafkaadmin.config.SecurityConfig;
import cl.rutaexpress.kafkaadmin.dto.TopicCreatedResponse;
import cl.rutaexpress.kafkaadmin.dto.TopicSummary;
import cl.rutaexpress.kafkaadmin.exception.GroupNotInactiveException;
import cl.rutaexpress.kafkaadmin.exception.ProtectedResourceException;
import cl.rutaexpress.kafkaadmin.service.KafkaAdminService;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(KafkaAdminController.class)
@Import(SecurityConfig.class)
@TestPropertySource(properties = {
        "TENANT_ID=00000000-0000-0000-0000-000000000000",
        "API_CLIENT_ID=00000000-0000-0000-0000-000000000000"
})
class KafkaAdminControllerTest {

    private static final SimpleGrantedAuthority ADMIN = new SimpleGrantedAuthority("Admin");
    private static final SimpleGrantedAuthority OPERADOR = new SimpleGrantedAuthority("Operador");

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private KafkaAdminService service;

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Test
    void listTopicsReturns200ForAdmin() throws Exception {
        when(service.listTopics(false)).thenReturn(List.of(
                new TopicSummary("shipments.events", 3, 3, 604800000L, "delete", 2, false, true)));

        mockMvc.perform(get("/api/kafka/topics").with(jwt().authorities(ADMIN)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("shipments.events"))
                .andExpect(jsonPath("$[0].protectedTopic").value(true));
    }

    @Test
    void anyRouteReturns403ForNonAdmin() throws Exception {
        mockMvc.perform(get("/api/kafka/topics").with(jwt().authorities(OPERADOR)))
                .andExpect(status().isForbidden());
        mockMvc.perform(delete("/api/kafka/topics/pruebas.demo").with(jwt().authorities(OPERADOR)))
                .andExpect(status().isForbidden());
        verifyNoInteractions(service);
    }

    @Test
    void anyRouteReturns401WithoutToken() throws Exception {
        mockMvc.perform(get("/api/kafka/consumer-groups"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void createTopicReturns201() throws Exception {
        when(service.createTopic(any())).thenReturn(new TopicCreatedResponse("pruebas.demo", 3, 3, Map.of()));

        mockMvc.perform(post("/api/kafka/topics")
                        .with(jwt().authorities(ADMIN))
                        .contentType("application/json")
                        .content("{\"name\":\"pruebas.demo\",\"partitions\":3,\"replicationFactor\":3}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("pruebas.demo"));
    }

    @Test
    void createTopicValidatesNamePartitionsAndReplicas() throws Exception {
        mockMvc.perform(post("/api/kafka/topics")
                        .with(jwt().authorities(ADMIN))
                        .contentType("application/json")
                        .content("{\"name\":\"no valido!\",\"partitions\":0,\"replicationFactor\":4}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.fields.name").exists())
                .andExpect(jsonPath("$.fields.partitions").exists())
                .andExpect(jsonPath("$.fields.replicationFactor").exists());
        verifyNoInteractions(service);
    }

    @Test
    void createTopicReturns409WhenItAlreadyExists() throws Exception {
        when(service.createTopic(any())).thenThrow(new TopicExistsException("Ya existe un tópico llamado 'pruebas.demo'"));

        mockMvc.perform(post("/api/kafka/topics")
                        .with(jwt().authorities(ADMIN))
                        .contentType("application/json")
                        .content("{\"name\":\"pruebas.demo\",\"partitions\":3,\"replicationFactor\":3}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("TOPIC_ALREADY_EXISTS"));
    }

    @Test
    void describeTopicReturns404ForUnknownTopic() throws Exception {
        when(service.describeTopic("no.existe")).thenThrow(new UnknownTopicOrPartitionException("unknown"));

        mockMvc.perform(get("/api/kafka/topics/no.existe").with(jwt().authorities(ADMIN)))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("TOPIC_NOT_FOUND"));
    }

    @Test
    void deleteProtectedTopicReturns409() throws Exception {
        doThrow(new ProtectedResourceException("protegido")).when(service).deleteTopic("shipments.events");

        mockMvc.perform(delete("/api/kafka/topics/shipments.events").with(jwt().authorities(ADMIN)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("PROTECTED_RESOURCE"));
    }

    @Test
    void updateConfigsReturns204AndPassesTheTopicNameWithDots() throws Exception {
        mockMvc.perform(put("/api/kafka/topics/audit.timeline/configs")
                        .with(jwt().authorities(ADMIN))
                        .contentType("application/json")
                        .content("{\"set\":{\"retention.ms\":\"259200000\"}}"))
                .andExpect(status().isNoContent());

        verify(service).updateTopicConfigs(eq("audit.timeline"), any());
    }

    @Test
    void increasePartitionsRequiresTotalPartitions() throws Exception {
        mockMvc.perform(post("/api/kafka/topics/pruebas.demo/partitions")
                        .with(jwt().authorities(ADMIN))
                        .contentType("application/json")
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fields.totalPartitions").exists());
    }

    @Test
    void resetRejectsUnknownStrategy() throws Exception {
        mockMvc.perform(post("/api/kafka/consumer-groups/ms-rutaexpress-audit/reset")
                        .with(jwt().authorities(ADMIN))
                        .contentType("application/json")
                        .content("{\"strategy\":\"MIDDLE\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("MALFORMED_REQUEST"));
    }

    @Test
    void resetOnActiveGroupReturns409() throws Exception {
        when(service.resetOffsets(eq("ms-rutaexpress-audit"), any()))
                .thenThrow(new GroupNotInactiveException("activo"));

        mockMvc.perform(post("/api/kafka/consumer-groups/ms-rutaexpress-audit/reset")
                        .with(jwt().authorities(ADMIN))
                        .contentType("application/json")
                        .content("{\"strategy\":\"EARLIEST\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("GROUP_NOT_INACTIVE"));
    }

    @Test
    void deleteConsumerGroupReturns204() throws Exception {
        mockMvc.perform(delete("/api/kafka/consumer-groups/grupo-de-prueba").with(jwt().authorities(ADMIN)))
                .andExpect(status().isNoContent());

        verify(service).deleteConsumerGroup("grupo-de-prueba");
    }
}
