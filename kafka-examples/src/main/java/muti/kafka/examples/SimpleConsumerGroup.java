package muti.kafka.examples;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Kafka Simple Consumer Group
 * 
 * Consumer group enables for a multi-threaded or 
 * multi-machine consumption from Kafka topics.
 * 
 * The maximum level of parallelism of a group is defined by 
 * the number of consumers in the group --> no of partitions.
 * 
 * Kafka assigns the partitions of a topic to the consumers in a group 
 * such that each partition is consumed by exactly one consumer in the group.
 * 
 * Kafka guarantees that a message is only ever read by a single consumer in the group.
 * Consumers can see the message in the order they were stored in the log.
 *
 * @author Andrea Muti
 * created: 01 ott 2017
 *
 */
public class SimpleConsumerGroup {

	public static void main(String[] args) throws Exception {

		args = new String[2];
		args[0] = "myTopic";
		args[1] = "myGroup";

		if (args.length < 2) {
			System.out.println("Usage: consumer <topic> <groupname>");
			return;
		}

		String topic = args[0].toString();
		String group = args[1].toString();

		Properties props = new Properties();
		
		props.put("bootstrap.servers", "localhost:9092");
		
		// Consumers can join a group by using the same group.id.
		props.put("group.id", group);
		
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		consumer.subscribe(Arrays.asList(topic));
		System.out.println("Subscribed to topic " + topic);

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
			}
		}     
	}  
}