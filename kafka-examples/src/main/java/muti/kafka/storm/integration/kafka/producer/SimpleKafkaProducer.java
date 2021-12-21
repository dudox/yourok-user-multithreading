package muti.kafka.storm.integration.kafka.producer;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

/**
 * Kafka Simple Producer.
 * Publishes on topic "my-first-topic" used in the Storm-Integration example.
 *
 * @author Andrea Muti
 * created: 01 ott 2017
 *
 */
public class SimpleKafkaProducer {

	public static void main(String[] args) {

		String topicName = "my-first-topic";

		Properties props = new Properties();
		props.put("client.id", "SimpleKafkaStormApp");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = null;
		try {
			producer = new KafkaProducer<String, String>(props);

			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(1), "hello"));
			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(1), "kafka"));
			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(1), "storm"));
			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(1), "spark"));
			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(1), "test message"));
			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(1), "another test message"));

			System.out.println("Messages sent successfully");
		}
		catch (Exception e) {
			System.err.println("ERROR while sending messages: "+e.getMessage());
		}
		finally {

			producer.flush();
			producer.close();

			Map<MetricName, ? extends Metric> metrics = producer.metrics();
			System.out.println(" --- metrics ---");
			for (MetricName m : metrics.keySet()) {
				System.out.println(" - name:"+m.name() + " - description:"+m.description()+" - value:" + metrics.get(m).value());
			}

		}

	}
}