package muti.kafka.spark.integration;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * SparkStringConsumer
 * @author Andrea Muti
 * created: 11 nov 2017
 *
 */
public class SparkStringConsumer {

	public static void main(String[] args) throws InterruptedException {

		System.out.println(" --- spark string consumer ---");

		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf()
				.setAppName("Consumer")
				.setMaster("local[*]");

		try (JavaSparkContext context = new JavaSparkContext(conf);
				
				// We have configured the period to 2 seconds (2000 ms). 
				// period = frequency of the micro-batching
				// Notice that Spark Streaming is not designed for periods shorter than about half a second. 
				// If you need a shorter delay in your processing, try Flink or Storm instead.
				JavaStreamingContext streamingContext = new JavaStreamingContext(context, new Duration(2000))) {

			Map<String, String> kafkaParams = new HashMap<>();
			kafkaParams.put("metadata.broker.list", "localhost:9092");
			Set<String> topics = Collections.singleton("spark-kafka-topic");

			JavaPairInputDStream<String, String> stream =
					KafkaUtils.createDirectStream(streamingContext, String.class, String.class,
							StringDecoder.class, StringDecoder.class, kafkaParams, topics);

			stream.foreachRDD(rdd -> {
				
				System.out.println("--- New RDD with " + rdd.partitions().size() + " partitions and " + rdd.count() + " records");
				
				rdd.foreach(record -> {
					System.out.println(record._1() + ": " + record._2());
				});
			});

			streamingContext.start();
			streamingContext.awaitTermination();
		}
	}

}