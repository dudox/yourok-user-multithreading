package muti.kafka.multi.threaded.example.singleconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * ConsumerThreadHandler.
 * 
 * This thread processes the message dispatched from the consumer. 
 * In this example, it simply print out the messages, offsets on the topic and the current ThreadID.
 * 
 * @author Andrea Muti
 * created: 26 nov 2017
 *
 */
public class ConsumerThreadHandler implements Runnable {

	private ConsumerRecord<String, String> consumerRecord;

	public ConsumerThreadHandler(ConsumerRecord<String, String> consumerRecord) {
		this.consumerRecord = consumerRecord;
	}

	public void run() {
		System.out.println("[Consumer] Process: " + consumerRecord.value() + ", Offset: " + consumerRecord.offset()
		+ ", By ThreadID: " + Thread.currentThread().getId());
	}
}