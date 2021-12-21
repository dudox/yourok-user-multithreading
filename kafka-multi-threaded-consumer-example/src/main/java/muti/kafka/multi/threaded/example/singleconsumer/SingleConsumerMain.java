package muti.kafka.multi.threaded.example.singleconsumer;

/**
 * SingleConsumerMain.
 * 
 * The entry point, contains the main method to run the example. 
 * It create a NotificationProducerThread thread which produces 5 messages to the topic: HelloKafkaTopic. 
 * Then, it creates a NotificationConsumer object which will receive the message from the above topic and 
 * dispatch to the pool of 3 ConsumerThreadHandler thread for processing.
 *
 * @author Andrea Muti
 * created: 26 nov 2017
 *
 */
public final class SingleConsumerMain {

	public static void main(String[] args) {

		String brokers = "localhost:9092";
		String groupId = "group01";
		String topic = "HelloKafkaTopic";
		int numberOfThread = 3;

		if (args != null && args.length > 4) {
			brokers = args[0];
			groupId = args[1];
			topic = args[2];
			numberOfThread = Integer.parseInt(args[3]);
		}

		// Start Notification Producer Thread
		NotificationProducerThread producerThread = new NotificationProducerThread(brokers, topic);
		Thread t1 = new Thread(producerThread);
		t1.start();

		// Start group of Notification Consumer Thread
		NotificationConsumer consumers = new NotificationConsumer(brokers, groupId, topic);

		consumers.execute(numberOfThread);

		try {
			Thread.sleep(100000);
		} catch (InterruptedException ie) {}
		
		consumers.shutdown();
	}
}