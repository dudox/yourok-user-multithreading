package muti.kafka.custom.serializer.user;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * UserDeserializer.
 * 
 * Implementation of the Deserializer of the Apache Kafka client API. 
 * We need to implement the method 'deserialize'. 
 * We utilize the ObjectMaper object to convert the byte array back to the User object.
 * 
 * @author Andrea Muti
 * created: 26 nov 2017
 *
 */
public class UserDeserializer implements Deserializer<User> {

	@Override
	public void close() {

	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {

	}

	@Override
	public User deserialize(String arg0, byte[] arg1) {
		
		ObjectMapper mapper = new ObjectMapper();
		User user = null;
		try {
			user = mapper.readValue(arg1, User.class);
		} catch (Exception e) {

			e.printStackTrace();
		}
		return user;
	}

}