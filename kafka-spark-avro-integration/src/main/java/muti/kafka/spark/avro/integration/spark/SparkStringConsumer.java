package muti.kafka.spark.avro.integration.spark;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.serializer.StringDecoder;

/**
 * SparkStringConsumer.
 *
 * @author Andrea Muti
 * created: 21 gen 2018
 *
 */
public class SparkStringConsumer {

	private static final Logger logger = LoggerFactory.getLogger(SparkStringConsumer.class);
	
	private static final String TOPIC_NAME = "mytopic";

	public static void main(String[] args) {

		// create spark configuration
		SparkConf conf = new SparkConf()
				.setAppName("kafka-sandbox")
				.setMaster("local[*]");
		
		// initialize spark context
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// initialize streaming context
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

		// processing pipeline
		
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton(TOPIC_NAME);

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
		        String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		
		directKafkaStream.foreachRDD(rdd -> {
		    logger.info("--- New RDD with {} partitions and {} records", rdd.partitions().size(), rdd.count());
		    rdd.foreach(record -> logger.info("Record value: {}", record._2));
		});

		// start the StreamingContext and keep the application alive.
		ssc.start();
		logger.info("Spark Streaming App started");
		
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {}
		
		// close the streaming context
		ssc.close();
		logger.info("Spark Streaming App closed");

	}
}