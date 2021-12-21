package muti.kafka.examples;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

/**
 * Kafka Simple Producer.
 *
 * @author Andrea Muti
 * created: 01 ott 2017
 *
 */
public class SimpleProducer {

	public static void main(String[] args) {

		args = new String[1];
		args[0] = "myTopic";

		// Check arguments length value
		if (args.length == 0) {
			System.out.println("Enter topic name");
			return;
		}

		// Assign topicName to string variable
		String topicName = args[0].toString();

		// create instance for properties to access producer configs   
		Properties props = new Properties();
		
		// identifies producer application
		props.put("client.id", "SimpleKafkaApp");
		
		// either sync or async
//		props.put("producer.type", "sync");
		
		// bootstrap.servers is the IP addresses of Kafka cluster.
		// If you have more than 1 broker, you can put all separated by commas.
		// Assign localhost id
		props.put("bootstrap.servers", "localhost:9092");

		// Set acknowledgements for producer requests.
		// The acks config controls the criteria under producer 
		// requests are considered complete.
		props.put("acks", "all");

		// If the request fails, the producer can automatically retry,
		props.put("retries", 0);

		// Specify buffer size in config
		props.put("batch.size", 16384);

		// Reduce the no of requests less than 0
		// If you want to reduce the number of requests you can 
		// set linger.ms to something greater than some value.
		props.put("linger.ms", 1);

		// The buffer.memory controls the total amount of memory available to the producer for buffering.   
		props.put("buffer.memory", 33554432);
		
		// Kafka message sent to Kafka cluster is combined with key (optional) and value which 
		// can be any data type. We need to specify how Kakfa producer should serialize those data 
		// types into binary before sending to Kafka cluster. Here, we will produce text messages 
		// to Kafka cluster. Therefore, we use StringSerializer which is a built-in serializer of 
		// Kafka client to serialize strings into binary.
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = null;
		try {
			producer = new KafkaProducer<String, String>(props);
			String msg;
			for (int i = 0; i < 100; i++) {
				
				// the KafkaProducer class provides send method to send messages asynchronously to a topic
				// - first argument is ProducerRecord: the producer manages a buffer of records 
				//   waiting to be sent.
				// - second argument (optional) is Callback: A user-supplied callback to execute 
				//   when the record has been acknowledged by the server (null, or no argument 
				//   at all, indicates no callback).
				
				msg = "Message " + i;
				producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), msg));
				Thread.sleep(1000);
			}

			System.out.println("Messages sent successfully");
		}
		catch (Exception e) {
			System.err.println("ERROR while sending messages: "+e.getMessage());
		}
		finally {
			
			
			// KafkaProducer class provides a flush method to ensure all 
			// previously sent messages have been actually completed
			producer.flush();
			
			// Blocks until all previously sent requests are completed.
			producer.close();
			
			Map<MetricName, ? extends Metric> metrics = producer.metrics();
			System.out.println(" --- metrics ---");
			for (MetricName m : metrics.keySet()) {
				System.out.println(" - name:"+m.name() + " - description:"+m.description()+" - value:" + metrics.get(m).value());
			}

		}

	}
}