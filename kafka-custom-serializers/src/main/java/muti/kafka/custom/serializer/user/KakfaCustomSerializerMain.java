package muti.kafka.custom.serializer.user;

/**
 * KakfaCustomSerializerMain.
 * 
 * The entry point class which initialize the producer, consumer threads for our testing purpose.
 *
 * @author Andrea Muti
 * created: 26 nov 2017
 *
 */
public final class KakfaCustomSerializerMain {

	public static void main(String[] args) {

		String brokers = "localhost:9092";
		String groupId = "group01";
		String topic = "UserMessageTopic10";

		if (args != null && args.length == 3) {
			brokers = args[0];
			groupId = args[1];
			topic = args[2];
		}

		// Start User Producer Thread
		UserProducerThread producerThread = new UserProducerThread(brokers, topic);
		Thread t1 = new Thread(producerThread);
		t1.start();

		// Start group of User Consumer Thread
		UserConsumerThread consumerThread = new UserConsumerThread(brokers, groupId, topic);
		Thread t2 = new Thread(consumerThread);
		t2.start();

		try {
			Thread.sleep(100000);
		} catch (InterruptedException ie) {

		}
	}
}