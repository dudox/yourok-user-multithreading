package muti.kafka.spark.avro.integration.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * SimpleStringConsumer.
 * 
 * Simple Kafka consumer of String messages sent to the "mytopic" topic.
 *
 * @author Andrea Muti
 * created: 21 gen 2018
 *
 */
public class SimpleStringConsumer {
	
	private static final String TOPIC_NAME = "mytopic";
	
	private static final Logger logger = LoggerFactory.getLogger(SimpleStringConsumer.class);

	public static void main(String[] args) {
	
		logger.info("Start Kafka String Consumer");
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "mygroup");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(TOPIC_NAME));
		logger.info("String Consumer subscribed to topic: {}", TOPIC_NAME);

		boolean running = true;
		while (running) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				logger.info("Received Record with content: {}", record.value());
			}
		}

		consumer.close();
		logger.info("Closed Kafka Consumer");

	}
}