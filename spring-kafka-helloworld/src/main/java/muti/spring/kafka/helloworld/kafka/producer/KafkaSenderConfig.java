package muti.spring.kafka.helloworld.kafka.producer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * KafkaSenderConfig.
 *
 * @author Andrea Muti
 * created: 21 gen 2018
 *
 */
@Configuration
public class KafkaSenderConfig {

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Bean
	public Map<String, Object> producerConfigs() {
		
		Map<String, Object> props = new HashMap<>();
		
		// list of host:port pairs used for establishing the initial connections to the Kakfa cluster
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		return props;
	}

	/**
	 * Creates a ProducerFactory.
	 * 
	 * In order to be able to use the Spring Kafka template, a ProducerFactory 
	 * must be configured and provided in the template's constructor. 
	 * The producer factory needs to be set with a number of mandatory properties,
	 * retrieved by the {@code producerConfigs()} method.
	 * 
	 * @return a ProducerFactory object.
	 */
	@Bean
	public ProducerFactory<String, String> producerFactory() {

		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {

		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public KafkaSender sender() {

		return new KafkaSender();
	}
}