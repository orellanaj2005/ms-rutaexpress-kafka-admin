package cl.rutaexpress.kafkaadmin;

import cl.rutaexpress.kafkaadmin.config.KafkaAdminProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(KafkaAdminProperties.class)
public class KafkaadminApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaadminApplication.class, args);
	}

}
