package muti.spring.kafka.helloworld.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * KafkaSender.
 *
 * @author Andrea Muti
 * created: 21 gen 2018
 *
 */
public class KafkaSender {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSender.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	/**
	 * Sends a String message over the topic whose name is provided as input argument.
	 *
	 * @param topic a String representing the topic name where the message has to be sent.
	 * @param payload a String representing the message to be sent.
	 */
	public void send(String topic, String payload) {

		LOGGER.info("sending payload='{}' to topic='{}'", payload, topic);
		kafkaTemplate.send(topic, payload);
	}
}