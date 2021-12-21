package muti.spring.kafka.helloworld.kafka.consumer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * KafkaReceiver.
 *
 * @author Andrea Muti
 * created: 21 gen 2018
 *
 */
public class KafkaReceiver {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiver.class);

	/**
	 * For testing convenience, a CountDownLatch is used. 
	 * This allows the POJO to signal that a message is received. 
	 * This is something you are not likely to implement in a production application.
	 */
	private CountDownLatch latch = new CountDownLatch(1);
	
	/**
	 * Returns the CountDownLatch of this Kafka Receiver.
	 *
	 * @return a {@link CountDownLatch} object.
	 */
	public CountDownLatch getLatch() {

		return latch;
	}

	/**
	 * Method called upon receival of a message.
	 * 
	 * The @KafkaListener annotation creates a ConcurrentMessageListenerContainer 
	 * message listener container behind the scenes for each annotated method.
	 * Using the topics element of the annotation, it is possible to specity the 
	 * topics for this listener. The name of the topic is injected from the application.yml.
	 *
	 * @param payload
	 */
	@KafkaListener(topics = "${kafka.topic.helloworld}")
	public void receive(String payload) {

		LOGGER.info("received payload='{}'", payload);
		latch.countDown();
	}
}