package muti.spring.kafka.helloworld.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/**
 * KafkaReceiverConfig.
 * 
 * It groups the creation and configuration of the different Spring Beans needed 
 * for the {@link KafkaReceiver}. The @EnableKafka annotation enables the detection 
 * of the @KafkaListener annotation that is used on the {@link KafkaReceiver} class.
 * 
 * @author Andrea Muti
 * created: 21 gen 2018
 *
 */
@Configuration
@EnableKafka
public class KafkaReceiverConfig {

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Bean
	public Map<String, Object> consumerConfigs() {
		
		Map<String, Object> props = new HashMap<>();
		
		// list of host:port pairs used for establishing the initial connections to the Kafka cluster
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		
		// allows a pool of processes to divide the work of consuming and processing records.
		// Messages will effectively be load balanced over consumer instances that have the same group id.
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "helloworld");
		
		// automatically reset the offset to the earliest offset.
		// This ensures that our consumer reads from the beginning of the topic 
		// even if some messages were already sent before it was able to startup.
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return props;
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {

		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	}
	
	/**
	 * Returns a KafkaListenerConnectionFactory.
	 * 
	 * This method is used by the @KafkaListener annotation from the KafkaReceiver 
	 * in order to configure a {@link MessageListenerContainer}. 
	 * In order to create it, a {@link ConsumerFactory} and accompanying configuration Map is needed.
	 * @return
	 */
	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
		
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());

		return factory;
	}

	@Bean
	public KafkaReceiver receiver() {

		return new KafkaReceiver();
	}
}