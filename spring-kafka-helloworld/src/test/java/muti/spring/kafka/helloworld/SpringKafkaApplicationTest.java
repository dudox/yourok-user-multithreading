package muti.spring.kafka.helloworld;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import muti.spring.kafka.helloworld.kafka.consumer.KafkaReceiver;
import muti.spring.kafka.helloworld.kafka.producer.KafkaSender;

/**
 * SpringKafkaApplicationTest.
 * 
 * Simple Test to verify the successful send and receive a message to and from Apache Kafka.
 *
 * @author Andrea Muti
 * created: 22 gen 2018
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class SpringKafkaApplicationTest {

	private static final String HELLOWORLD_TOPIC = "helloworld.topic";
	
	// An embedded Kafka broker is automatically started by using a @ClassRule annotation
	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, HELLOWORLD_TOPIC);

	@Autowired
	private KafkaReceiver receiver;

	@Autowired
	private KafkaSender sender;
	
	/**
	 * Simple Unit test case that uses the {@link KafkaSender} bean to send a 
	 * message to the 'helloworld.topic' topic on the Kafka broker.
	 * If the {@link CountDownLatch} from the {@link KafkaReceiver} was lowered 
	 * from 1 to 0 as this indicates a message was processed by the KafkaReceiver's 
	 * {@code receive()} method.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testReceive() throws Exception {

		sender.send(HELLOWORLD_TOPIC, "Hello Spring Kafka!");

		receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);

		assertThat(receiver.getLatch().getCount()).isEqualTo(0);
	}
}