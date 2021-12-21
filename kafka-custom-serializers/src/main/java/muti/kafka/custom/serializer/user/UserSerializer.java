package muti.kafka.custom.serializer.user;

import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * UserSerializer.
 * 
 * We have to implement the Serializer interface of Apache Kafka client API. 
 * The main method we need to implement is the 'serialize'  method. 
 * It simply converts the object into byte array and returns. 
 * In here, we utilize the ObjectMapper object from the jackson-databind library 
 * to quickly serialize the whole object into byte array.
 *
 * @author Andrea Muti
 * created: 26 nov 2017
 *
 */
public class UserSerializer implements Serializer<User> {

	@Override
	public void close() {

	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {

	}

	@Override
	public byte[] serialize(String arg0, User arg1) {
		
		byte[] retVal = null;
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			retVal = objectMapper.writeValueAsString(arg1).getBytes();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}

}