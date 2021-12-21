package muti.kafka.multi.threaded.example.multipleconsumers;

/**
 * MultipleConsumersMain.
 * 
 * This class is the entry point, contain the main method for testing our source code. 
 * In this class we will create a producer thread to produces 5 messages to the topic 
 * which has 3 partitions:HelloKafkaTopic1.
 * 
 * Then we create a group of 3 consumers on their own thread to consume the message 
 * from the HelloKafkaTopic1 topic.
 * 
 * @author Andrea Muti
 * created: 26 nov 2017
 *
 */
public final class MultipleConsumersMain {

	public static void main(String[] args) {

		String brokers = "localhost:9092";
		String groupId = "group01";
		String topic = "HelloKafkaTopic1";
		int numberOfConsumer = 3;

		if (args != null && args.length > 4) {
			brokers = args[0];
			groupId = args[1];
			topic = args[2];
			numberOfConsumer = Integer.parseInt(args[3]);
		}

		// Start Notification Producer Thread
		NotificationProducerThread producerThread = new NotificationProducerThread(brokers, topic);
		Thread t1 = new Thread(producerThread);
		t1.start();

		// Start group of Notification Consumers
		NotificationConsumerGroup consumerGroup = new NotificationConsumerGroup(brokers, groupId, topic, numberOfConsumer);

		consumerGroup.execute();

		try {
			Thread.sleep(100000);
		} catch (InterruptedException ie) {}
	}
}