package muti.kafka.multi.threaded.example.multipleconsumers;

import java.util.ArrayList;
import java.util.List;

/**
 * NotificationConsumerGroup.
 * Group of NotificationConsumerThread(s)	
 *
 * This class creates a group of consumer threads based on the given parameters:
 *  - brokers: The Kafka brokers to which consumers group will connect
 *  - groupId: The group id. All consumers on this group will have the same groupId
 *  - topic: The topic to which the consumers group will fetch the data
 *  - numberOfConsumer: the number of consumers will be created for the group
 * 
 * @author Andrea Muti
 * created: 26 nov 2017
 *
 */
public class NotificationConsumerGroup {

	private final int numberOfConsumers;
	private final String groupId;
	private final String topic;
	private final String brokers;
	private List<NotificationConsumerThread> consumers;

	public NotificationConsumerGroup(String brokers, String groupId, String topic, int numberOfConsumers) {

		this.brokers = brokers;
		this.topic = topic;
		this.groupId = groupId;
		this.numberOfConsumers = numberOfConsumers;
		consumers = new ArrayList<>();

		for (int i = 0; i < this.numberOfConsumers; i++) {

			NotificationConsumerThread ncThread = new NotificationConsumerThread(this.brokers, this.groupId, this.topic);
			consumers.add(ncThread);
		}
	}

	public void execute() {

		for (NotificationConsumerThread ncThread : consumers) {

			Thread t = new Thread(ncThread);
			t.start();
		}
	}

	/**
	 * @return the numberOfConsumers
	 */
	public int getNumberOfConsumers() {

		return numberOfConsumers;
	}

	/**
	 * @return the groupId
	 */
	public String getGroupId() {

		return groupId;
	}

}