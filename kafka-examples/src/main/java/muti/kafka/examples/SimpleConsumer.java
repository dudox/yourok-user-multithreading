package muti.kafka.examples;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Kafka Simple Consumer.
 *
 * @author Andrea Muti
 * created: 01 ott 2017
 *
 */
public class SimpleConsumer {
	
	public static void main(String[] args) {
		
		args = new String[1];
		args[0] = "myTopic";
		
		if (args.length == 0) {
			System.out.println("Enter topic name");
			return;
		}

		// Kafka consumer configuration settings
		String topicName = args[0].toString();
		Properties props = new Properties();

		// Bootstrapping list of brokers.
		props.put("bootstrap.servers", "localhost:9092");
		
		// It is the the group id of processes which the consumer belonged to.
		props.put("group.id", "test");
		
		// Enable auto commit for offsets if the value is true, otherwise not committed.
		props.put("enable.auto.commit", "true");
		
		// Return how often updated consumed offsets are written to ZooKeeper.
		props.put("auto.commit.interval.ms", "1000");
		
		// Indicates how many milliseconds Kafka will wait for the ZooKeeper to respond 
		// to a request (read or write) before giving up and continuing to consume messages.
		props.put("session.timeout.ms", "30000");
		
		// They are deserializers used by Kafka consumer to deserialize the binary data received 7
		// from Kakfa cluster, to our desire data types. In this example, because the producer produces 
		// string message, our consumer use StringDeserializer which is a  built-in  deserialize  of 
		// Kafka client API to deserialize the binary data to string.
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		// Kafka Consumer subscribes list of topics here.
		consumer.subscribe(Arrays.asList(topicName));

		// print the topic name
		System.out.println("Subscribed to topic " + topicName);
	
		while (true) {

			// Our consumer polls the clusters for the new messages. 
			// The method takes a timeout =100 milliseconds which is the time the 
			// consumers waits if there is no message from cluster. 
			ConsumerRecords<String, String> records = consumer.poll(100);
			
			// The number of records for all the topics.
			System.out.println("\n- record count (for all topics): "+records.count());
			
			// The set of partitions with data in this record set 
			// (if no data was returned then the set is empty).
			System.out.println("- partitions number: "+records.partitions().size());
			
			for (ConsumerRecord<String, String> record : records) {

				// print the offset, key and value for the consumer records.
				System.out.printf("- offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
			}
		}
	}
}