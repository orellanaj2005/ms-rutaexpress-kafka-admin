package cl.rutaexpress.kafkaadmin;

import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Verifica que el contexto de Spring arranca. Se entregan valores ficticios para
 * las variables de Azure AD y se reemplazan el JwtDecoder y el AdminClient por mocks,
 * para que la prueba no dependa de internet ni de un clúster Kafka.
 */
@SpringBootTest(properties = {
		"TENANT_ID=00000000-0000-0000-0000-000000000000",
		"API_CLIENT_ID=00000000-0000-0000-0000-000000000000"
})
class KafkaadminApplicationTests {

	@MockitoBean
	private JwtDecoder jwtDecoder;

	@MockitoBean
	private AdminClient adminClient;

	@Test
	void contextLoads() {
	}

}
