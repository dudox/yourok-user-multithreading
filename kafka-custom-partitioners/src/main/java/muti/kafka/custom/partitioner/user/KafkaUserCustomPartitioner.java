package muti.kafka.custom.partitioner.user;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * KafkaUserCustomPartitioner.
 * 
 * This is the custom partitioner. 
 * We need to implement the Partitioner interface. 
 * Our implementation will get the key which is the userName, retrieve the corresponding user Id 
 * and return it as the partition number.  
 * The messages sent to 
 *  - Tom will be stored on the partition #1, 
 *  - Mary: #2, 
 *  - Alice: #3, 
 *  - Daisy: #4 and 
 *  - Helen: #5 or 
 *  - partition #0 otherwise.
 * 
 * @author Andrea Muti
 * created: 26 nov 2017
 *
 */
public class KafkaUserCustomPartitioner implements Partitioner {
	
	private IUserService userService;

	public KafkaUserCustomPartitioner() {
		
		userService = new UserServiceImpl();
	}

	@Override
	public void configure(Map<String, ?> configs) {

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

		int partition = 0;
		String userName = (String) key;
		
		// Find the id of current user based on the username
		Integer userId = userService.findUserId(userName);
		
		// If the userId not found, default partition is 0
		if (userId != null) {
			partition = userId;
		}
		
		return partition;
	}

	@Override
	public void close() {

	}

}