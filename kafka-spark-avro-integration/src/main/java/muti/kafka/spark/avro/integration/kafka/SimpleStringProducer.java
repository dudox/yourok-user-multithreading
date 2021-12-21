package muti.kafka.spark.avro.integration.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * SimpleStringProducer.
 * 
 * Simple Kafka Producer that sends 100 messages in 
 * String format to the "mytopic" topic.
 *  
 * @author Andrea Muti
 * created: 21 gen 2018
 */
public class SimpleStringProducer {
	
	private static final String TOPIC_NAME = "mytopic";
	
	private static final Logger logger = LoggerFactory.getLogger(SimpleStringProducer.class);
	
    public static void main(String[] args) {
    	
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        logger.info("Created Kafka Producer of String messages");
        
        logger.info("Start sending 100 messages on Kafka topic {}", TOPIC_NAME);
        for (int i = 0; i < 100; i++) {
        	
        	String recordMessage = String.format("value-%s", i);
        	
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, recordMessage);
            producer.send(record);
            
            try {
				Thread.sleep(250);
			} catch (InterruptedException e) {}
        }
        logger.info("All messages were sent");

        producer.close();
        logger.info("Closed Kafka Producer");

    }
}