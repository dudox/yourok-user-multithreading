package muti.kafka.spark.avro.integration.spark;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import muti.kafka.spark.avro.integration.avro.AvroSchemaLoader;
import muti.simple.dm.Locationa;

/**
 * SparkAvroConsumer.
 *
 * @author Andrea Muti
 * created: 21 gen 2018
 *
 */
public class SparkAvroConsumer {

	private static final Logger logger = LoggerFactory.getLogger(SparkAvroConsumer.class);

	private static final String TOPIC_NAME = "mytopic";

	// Reader must be static in order to be used inside the rdd.foreach closure
	private static ReflectDatumReader<Object> reader;

	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf()
				.setAppName("kafka-sandbox")
				.setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

		Set<String> topics = Collections.singleton(TOPIC_NAME);
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");

		JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(ssc,
				String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, kafkaParams, topics);

		AvroSchemaLoader loader = new AvroSchemaLoader();
		Schema schema = loader.load(Locationa.class);
		reader = new ReflectDatumReader<>(schema);

		directKafkaStream.foreachRDD(rdd -> {
		    logger.info("--- Received New RDD with {} partitions and {} records", rdd.partitions().size(), rdd.count());
			rdd.foreach(avroRecord -> {

				BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroRecord._2, null);
				Locationa loc = (Locationa) reader.read(null, decoder);

				logger.info("Received Avro-Serialized Location: {}", loc.toString());
			});
		});

		ssc.start();
		logger.info("Spark Avro Consumer started");

		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {}

		// close the streaming context
		ssc.close();
		logger.info("Spark Avro Consumer streaming context closed");
	}
}