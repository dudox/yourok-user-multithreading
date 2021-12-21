package muti.kafka.spark.avro.integration.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import muti.kafka.spark.avro.integration.spark.SparkStringConsumer;
import muti.simple.dm.Locationa;

/**
 * SimpleAvroProducer.
 * 
 * Simple Producer of Kafka messages whose payload is Avro serialized.
 *
 * @author Andrea Muti
 * created: 21 gen 2018
 *
 */
public class SimpleAvroProducer {

	private static final Logger logger = LoggerFactory.getLogger(SparkStringConsumer.class);

	private static final String TOPIC_NAME = "mytopic";

	public static void main(String[] args) throws IOException {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
		logger.info("Kafka Producer initialized - topic: {}", TOPIC_NAME);

		AvroSchemaLoader loader = new AvroSchemaLoader();
		Schema schema = loader.load(Locationa.class);
		
		logger.info("Start sending Avro-serialized Location objects");
		for (int i = 0; i < 1000; i++) {

			byte[] bytes = generateAvroRecord(schema, i);

			ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC_NAME, bytes);
			producer.send(record);

			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {}
		}
		logger.info("All Avro-serialized Location objects were sent");

		producer.close();
		logger.info("Closed Kafka Producer");
	}

	private static byte[] generateAvroRecord(Schema schema, int i) throws IOException {

		byte[] byteData = null;

		ReflectDatumWriter<Object> writer = new ReflectDatumWriter<>(schema);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

		// Build AVRO message
		Locationa location = new Locationa();
		location.setVehicleId(new org.apache.avro.util.Utf8("VHC-00"+i).toString());
		location.setTimestamp(System.currentTimeMillis() / 1000L);
		location.setLatitude(51.687402);
		location.setLongitude(5.307759);

		writer.write(location, encoder);
		encoder.flush();

		byteData = out.toByteArray();
		out.close();

		return byteData;
	}
}